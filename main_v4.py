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
# 1. 宏观水位引擎 (Yahoo Finance)
# ==========================================
def get_macro_waterlevel():
    """极速并发抓取全球宏观资产"""
    tickers = {
        "美债(US10Y)": "^TNX",
        "BTC": "BTC-USD",
        "KOSPI": "^KS11",
        "黄金(XAU)": "GC=F",
        "纳指(NQ)": "NQ=F",
        "XBI": "XBI"
    }
    macro_strs = []
    
    def fetch_ticker(name, symbol):
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="2d")
            if len(hist) >= 1:
                close = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2] if len(hist) > 1 else close
                pct = ((close - prev) / prev) * 100
                
                # 格式化
                if name == "BTC" or name == "黄金(XAU)" or name == "纳指(NQ)":
                    val_str = f"${int(close):,}"
                elif name == "美债(US10Y)":
                    val_str = f"{close:.2f}%"
                else:
                    val_str = f"{int(close)}"
                    
                return f"{name}:{val_str}({pct:+.2f}%)"
        except:
            pass
        return f"{name}:暂无"

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(fetch_ticker, k, v): k for k, v in tickers.items()}
        for fut in concurrent.futures.as_completed(futures):
            res = fut.result()
            if res: macro_strs.append(res)
            
    return " | ".join(macro_strs)

# ==========================================
# 2. 极速盘口与量比核算
# ==========================================
def get_realtime_data(proxy_code: str, eod_vol_str: str):
    """抓取腾讯极速API，并计算量比"""
    try:
        prefix = "sh" if proxy_code.startswith("5") else "sz"
        url = f"http://qt.gtimg.cn/q={prefix}{proxy_code}"
        resp = fetch_with_timeout(requests.get, 3, url)
        if resp:
            data = resp.text.split('~')
            if len(data) > 40:
                current_price = float(data[3])
                pct_change = float(data[32])
                today_turnover = float(data[37]) * 10000  # 转换为元
                
                # 估算量比 (当前成交额 / 昨全天成交额 * 时间修正)
                vol_ratio_str = "量比未知"
                if eod_vol_str:
                    try:
                        eod_vol = float(eod_vol_str.replace(",", ""))
                        now_bj = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
                        market_open = now_bj.replace(hour=9, minute=30, second=0)
                        minutes_passed = (now_bj - market_open).total_seconds() / 60
                        if 120 < minutes_passed < 210: minutes_passed -= 90 # 午休扣除
                        minutes_passed = max(1, min(240, minutes_passed))
                        
                        vol_ratio = (today_turnover / minutes_passed) / (eod_vol / 240)
                        vol_tag = "放量" if vol_ratio > 1.2 else "缩量" if vol_ratio < 0.8 else "平量"
                        vol_ratio_str = f"{vol_tag}({vol_ratio:.2f})"
                    except:
                        pass
                        
                return current_price, pct_change, vol_ratio_str
    except:
        pass
    return None, None, "无量比"

# ==========================================
# 3. 组装空战情报 (硬排版生成)
# ==========================================
def collect_v4_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    macro_str = get_macro_waterlevel()
    hard_data_display = [] # 纯粹给微信显示的硬核数据
    ai_prompt_etfs, ai_prompt_rules = [], []
    
    idx_share = get_idx("持有份额")
    idx_eod_vol = get_idx("[EOD]昨成交额")
    
    # 核心持仓读取
    for row in dash_data[1:]:
        if not row or (get_idx("基金代码") != -1 and not row[get_idx("基金代码")].strip().isdigit()): continue
        
        name = row[get_idx("基金名称")]
        proxy = row[get_idx("替身代码 (ETF)")]
        rule_limit = row[get_idx("定量证伪底线")]
        rule_logic = row[get_idx("定性持仓逻辑")]
        
        shares_str = row[idx_share].strip() if idx_share != -1 else "0"
        eod_vol_str = row[idx_eod_vol].strip() if idx_eod_vol != -1 else ""
        try:
            shares = float(shares_str.replace(",", ""))
        except:
            shares = 0.0
            
        pos_status = "【已空仓】" if shares <= 0 else "【持仓中】"
        ma20_eod = float(row[get_idx("[EOD]MA20点位")]) if get_idx("[EOD]MA20点位") != -1 and row[get_idx("[EOD]MA20点位")] else 0.0
        
        current_price, today_pct, vol_str = get_realtime_data(proxy, eod_vol_str)
        
        # 计算持仓市值
        market_value = (shares * current_price) if current_price else 0.0
        mv_str = f"¥{market_value/1000:.1f}k" if market_value > 0 else "空仓"
        
        ma_status = "MA20:未知"
        if current_price and ma20_eod > 0:
            dist = ((current_price - ma20_eod) / ma20_eod) * 100
            ma_status = f"MA20乖离:{dist:+.2f}%"
            
        pct_str = f"{today_pct:+.2f}%" if today_pct is not None else "停牌"
        
        # 组装微信硬核数据行 (例如：永赢半导体: +0.84% | 仓:¥29.0k | 放量(1.51) | MA20乖离:+0.33%)
        display_line = f"**{name}**: {pct_str} | 仓:{mv_str} | {vol_str} | {ma_status}"
        if shares > 0 or today_pct is not None: # 空仓且没行情的就不显示了
            hard_data_display.append(display_line)
        
        # 喂给 AI 的隐藏数据
        ai_prompt_etfs.append(f"* **{name}**({proxy}) {pos_status}: 今日 {pct_str} | {ma_status} | 底线纪律:{rule_limit}")
        ai_prompt_rules.append(f"- **{name}**: {rule_logic}")

    md_prompt = f"""## 🎯 场内盘口状态
{chr(10).join(ai_prompt_etfs)}
## 🌍 宏观水位
{macro_str}
"""
    return md_prompt, "\n".join(ai_prompt_rules), macro_str, hard_data_display

# ==========================================
# 4. AI 首席风控官 (严格限制只出结论)
# ==========================================
def ask_v4_tactical_agent(md_prompt: str, rules_str: str) -> dict:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: 顶级对冲基金量化总监
当前是 14:45。请基于以下盘口和宏观数据，输出冷静专业的交易指令。

【输入盘口情报】：
{md_prompt}
【定性逻辑参考】：
{rules_str}

# 🔴 核心约束条件：
1. 【执行指令】中，**只输出需要立即采取实质性动作（止盈、止损、加仓、触及底线）的标的**。如果安全或空仓，不要列出。
2. 绝对不能使用“绞杀”、“冷血”等情绪化词汇。

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
# 6. 企业微信终极排版拼接 (Python硬拼接)
# ==========================================
def notify_wechat(macro_str, hard_data_list, ai_summary, orders_list, doc_link):
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if not robot_key: return

    hard_data_block = "\n".join([f"- {line}" for line in hard_data_list])
    
    # 如果没有需要操作的，显示一句话
    if not orders_list:
        orders_str = "> 🟢 全阵地状态稳定，今日无极值操作动作。"
    else:
        orders_str = "\n".join([f"> {o}" for o in orders_list])
    
    content = f"""🚀 **V4.0 战术中枢 | 14:45 盘中快照**

🌍 **宏观水位**:
`{macro_str}`

📊 **核心持仓矩阵**:
{hard_data_block}

🧠 **AI 首席风控官决断**:
> {ai_summary}

⚡ **立即执行指令**:
{orders_str}

🔗 **[点击查阅深度宏观复盘日记]({doc_link})**"""

    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    try:
        requests.post(f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}", json=payload)
    except:
        pass

if __name__ == "__main__":
    MY_DOC_ID = "1ydm84CsKPnM3uFB4iSJsQrJ2A-sHV38GCt9_KMRV4vY"
    
    creds = get_google_credentials()
    gc = gspread.authorize(creds)
    
    md_prompt, rules_str, macro_str, hard_data_list = collect_v4_intelligence(gc)
    ai_json = ask_v4_tactical_agent(md_prompt, rules_str)
    
    doc_link = update_google_doc(creds, ai_json["doc_full_report"], MY_DOC_ID)
    notify_wechat(macro_str, hard_data_list, ai_json["ai_summary"], ai_json["execution_orders"], doc_link)
