import os
import json
import datetime
import pytz
import gspread
import requests
import concurrent.futures
import yfinance as yf
from google import genai
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ==========================================
# 0. 基础设置与 API 鉴权
# ==========================================
def fetch_with_timeout(func, timeout_sec, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_sec)
        except Exception:
            return None

def get_google_credentials():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("缺失 GCP_SERVICE_ACCOUNT")
    creds_dict = json.loads(creds_json)
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/documents',
        'https://www.googleapis.com/auth/drive'
    ]
    return service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)

# ==========================================
# 1. 宏观水位引擎 (双轨输出：排版字符 + 纯数字)
# ==========================================
def get_macro_waterlevel():
    tickers = {
        "美债(US10Y)": "^TNX",
        "BTC": "BTC-USD",
        "KOSPI": "^KS11",
        "黄金(XAU)": "GC=F",
        "纳指(NQ)": "NQ=F",
        "XBI": "XBI"
    }
    macro_strs = []
    macro_raw_dict = {} # 给未来 AI 训练用的纯数字字典
    
    def fetch_ticker(name, symbol):
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="2d")
            if len(hist) >= 1:
                close = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2] if len(hist) > 1 else close
                pct = ((close - prev) / prev) * 100
                
                if name in ["BTC", "黄金(XAU)", "纳指(NQ)"]:
                    val_str = f"${int(close):,}"
                elif name == "美债(US10Y)":
                    val_str = f"{close:.2f}%"
                else:
                    val_str = f"{int(close)}"
                    
                return name, f"{name}:{val_str}({pct:+.2f}%)", round(close, 2), round(pct, 2)
        except:
            pass
        return name, f"{name}:暂无", None, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(fetch_ticker, k, v): k for k, v in tickers.items()}
        for fut in concurrent.futures.as_completed(futures):
            name, disp_str, raw_val, raw_pct = fut.result()
            macro_strs.append(disp_str)
            if raw_val is not None:
                macro_raw_dict[name] = {"value": raw_val, "pct": raw_pct}
            
    return " | ".join(macro_strs), macro_raw_dict

# ==========================================
# 2. 极速盘口与量比核算
# ==========================================
def get_realtime_data(proxy_code: str, eod_vol_str: str):
    try:
        prefix = "sh" if proxy_code.startswith("5") else "sz"
        url = f"http://qt.gtimg.cn/q={prefix}{proxy_code}"
        resp = fetch_with_timeout(requests.get, 3, url)
        if resp:
            data = resp.text.split('~')
            if len(data) > 40:
                current_price = float(data[3])
                pct_change = float(data[32])
                today_turnover = float(data[37]) * 10000 
                
                vol_ratio_str = "量比未知"
                raw_vol_ratio = None
                if eod_vol_str:
                    try:
                        eod_vol = float(eod_vol_str.replace(",", ""))
                        now_bj = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
                        market_open = now_bj.replace(hour=9, minute=30, second=0)
                        minutes_passed = (now_bj - market_open).total_seconds() / 60
                        if 120 < minutes_passed < 210: minutes_passed -= 90
                        minutes_passed = max(1, min(240, minutes_passed))
                        
                        raw_vol_ratio = (today_turnover / minutes_passed) / (eod_vol / 240)
                        vol_tag = "放量" if raw_vol_ratio > 1.2 else "缩量" if raw_vol_ratio < 0.8 else "平量"
                        vol_ratio_str = f"{vol_tag}({raw_vol_ratio:.2f})"
                    except:
                        pass
                        
                return current_price, pct_change, vol_ratio_str, raw_vol_ratio
    except:
        pass
    return None, None, "无量比", None

# ==========================================
# 3. 组装情报 (生成微信排版 + 提取纯净数据字典)
# ==========================================
def collect_v4_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    macro_str, macro_raw_dict = get_macro_waterlevel()
    hard_data_display = [] 
    ai_prompt_etfs, ai_prompt_rules = [], []
    portfolio_raw_list = [] # 给未来 AI 训练用的纯净持仓数据数组
    
    idx_share = get_idx("持有份额")
    idx_eod_vol = get_idx("[EOD]昨成交额")
    
    for row in dash_data[1:]:
        if not row or (get_idx("基金代码") != -1 and not row[get_idx("基金代码")].strip().isdigit()): continue
        
        name = row[get_idx("基金名称")]
        proxy = row[get_idx("替身代码 (ETF)")]
        rule_limit = row[get_idx("定量证伪底线")]
        rule_logic = row[get_idx("定性持仓逻辑")]
        
        shares_str = row[idx_share].strip() if idx_share != -1 else "0"
        eod_vol_str = row[idx_eod_vol].strip() if idx_eod_vol != -1 else ""
        try: shares = float(shares_str.replace(",", ""))
        except: shares = 0.0
            
        pos_status = "【已空仓】" if shares <= 0 else "【持仓中】"
        ma20_eod = float(row[get_idx("[EOD]MA20点位")]) if get_idx("[EOD]MA20点位") != -1 and row[get_idx("[EOD]MA20点位")] else 0.0
        
        current_price, today_pct, vol_str, raw_vol_ratio = get_realtime_data(proxy, eod_vol_str)
        
        market_value = (shares * current_price) if current_price else 0.0
        mv_str = f"¥{market_value/1000:.1f}k" if market_value > 0 else "空仓"
        
        ma_status = "MA20:未知"
        raw_ma_dist = None
        if current_price and ma20_eod > 0:
            raw_ma_dist = ((current_price - ma20_eod) / ma20_eod) * 100
            ma_status = f"MA20乖离:{raw_ma_dist:+.2f}%"
            
        pct_str = f"{today_pct:+.2f}%" if today_pct is not None else "停牌"
        
        # 1. 组装给微信的硬排版字符
        display_line = f"**{name}**: {pct_str} | 仓:{mv_str} | {vol_str} | {ma_status}"
        if shares > 0 or today_pct is not None:
            hard_data_display.append(display_line)
        
        # 2. 组装给大模型做决断的提示词
        ai_prompt_etfs.append(f"* **{name}**({proxy}) {pos_status}: 今日 {pct_str} | {ma_status} | 底线纪律:{rule_limit}")
        ai_prompt_rules.append(f"- **{name}**: {rule_logic}")

        # 3. 组装给未来自己做 AI 训练用的纯净数据
        portfolio_raw_list.append({
            "name": name,
            "status": "空仓" if shares <= 0 else "持仓",
            "market_value_k": round(market_value/1000, 2),
            "pct_change": today_pct,
            "vol_ratio": round(raw_vol_ratio, 2) if raw_vol_ratio else None,
            "ma20_divergence_pct": round(raw_ma_dist, 2) if raw_ma_dist else None
        })

    md_prompt = f"## 🎯 场内盘口状态\n{chr(10).join(ai_prompt_etfs)}\n## 🌍 宏观水位\n{macro_str}"
    
    return md_prompt, "\n".join(ai_prompt_rules), macro_str, hard_data_display, macro_raw_dict, portfolio_raw_list

# ==========================================
# 4. AI 首席风控官 (严格限制只出结论)
# ==========================================
def ask_v4_tactical_agent(md_prompt: str, rules_str: str) -> dict:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: 顶级对冲基金量化总监
当前是 14:45。请基于以下盘口和宏观数据，输出冷静专业的交易指令。必须结合比特币流动性、韩国KOSPI等进行分析。

【输入盘口情报】：
{md_prompt}
【定性逻辑参考】：
{rules_str}

# 🔴 核心约束条件：
1. 【执行指令】中，**只输出需要立即采取实质性动作（买入、止损、触及底线）的标的**。如果安全观望或空仓，绝对不要列出。
2. 绝对不能使用“绞杀”、“冷血”等情绪化词汇。客观专业。
3. 状态判定：如果标的标注了“【已空仓】”，绝对不能喊“卖出/清仓”。

# 输出要求 (必须是合法的 JSON 格式)：
{{
  "ai_summary": "用两句话总结今天的宏观状态和盘面异常点，以及整体操作定调。",
  "execution_orders": [
    "🚨 天弘机器人：平仓止损 | 已实质性跌破MA20生命线"
  ],
  "doc_full_report": "### 🌍 宏观诊断与阵地推演\\n(结合宏观水位数据、资金流向进行800字深度长篇复盘。)"
}}
    """
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(
            temperature=0.1,
            response_mime_type="application/json"
        )
    )
    return json.loads(response.text)

# ==========================================
# 5. Google Doc 固定阵地注入
# ==========================================
def update_google_doc(creds, report_text: str, target_doc_id: str) -> str:
    tz_bj = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_bj)
    date_str = now.strftime('%Y-%m-%d %H:%M')
    docs_service = build('docs', 'v1', credentials=creds)
    insert_text = f"\n\n=================================\n📅 {date_str} 盘中风控决断\n=================================\n{report_text}\n"
    requests = [{'insertText': {'location': {'index': 1}, 'text': insert_text}}]
    docs_service.documents().batchUpdate(documentId=target_doc_id, body={'requests': requests}).execute()
    return f"https://docs.google.com/document/d/{target_doc_id}/edit"

# ==========================================
# 6. 企业微信终极排版拼接 (纯硬数据直推)
# ==========================================
def notify_wechat(macro_str, hard_data_list, ai_summary, orders_list, doc_link):
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if not robot_key: return

    hard_data_block = "\n".join([f"- {line}" for line in hard_data_list])
    
    if not orders_list:
        orders_str = "> 🟢 全阵地状态稳定，今日无极值操作动作。"
    else:
        orders_str = "\n".join([f"> {o}" for o in orders_list])
    
    content = f"""🚀 **V4.0 战术中枢 | 14:45 盘中快照**

🌍 **宏观水位**:
`{macro_str}`

📊 **核心持仓矩阵**:
{hard_data_block}

🧠 **AI 决断**:
> {ai_summary}

⚡ **立即执行指令**:
{orders_str}

🔗 **[点击查阅深度宏观复盘日记]({doc_link})**"""

    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    try:
        requests.post(f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}", json=payload)
    except: pass

if __name__ == "__main__":
    MY_DOC_ID = "1ydm84CsKPnM3uFB4iSJsQrJ2A-sHV38GCt9_KMRV4vY"
    
    creds = get_google_credentials()
    gc = gspread.authorize(creds)
    
    # 获取硬数据、宏观数据，以及高度结构化的机器学习状态数据
    md_prompt, rules_str, macro_str, hard_data_list, macro_raw_dict, portfolio_raw_list = collect_v4_intelligence(gc)
    
    # 呼叫 AI 决策
    ai_json = ask_v4_tactical_agent(md_prompt, rules_str)
    
    # 写入 Docs 和推微信
    doc_link = update_google_doc(creds, ai_json["doc_full_report"], MY_DOC_ID)
    notify_wechat(macro_str, hard_data_list, ai_json["ai_summary"], ai_json["execution_orders"], doc_link)
    
    # 🗄️ 拼装给未来的终极机器学习 JSON 语料库
    archive_json = {
        "timestamp": datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        "state_macro": macro_raw_dict,
        "state_portfolio": portfolio_raw_list,
        "action_ai_decision": ai_json
    }
    
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/AI_Trade_Log_{datetime.datetime.now().strftime('%Y%m%d')}.json", "w", encoding="utf-8") as f:
        json.dump(archive_json, f, ensure_ascii=False, indent=2)
