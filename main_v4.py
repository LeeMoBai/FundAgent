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
# 1. 宏观水位引擎 (引入 QQQ 用于 QDII 参照)
# ==========================================
def get_macro_waterlevel():
    tickers = {
        "美债(US10Y)": "^TNX",
        "BTC": "BTC-USD",
        "KOSPI": "^KS11",
        "黄金(XAU)": "GC=F",
        "纳指(NQ)": "NQ=F",
        "纳指ETF(QQQ)": "QQQ", # 专供华宝纳斯达克提取“昨夜”数据
        "XBI": "XBI"
    }
    macro_strs = []
    macro_raw_dict = {} 
    
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(fetch_ticker, k, v): k for k, v in tickers.items()}
        for fut in concurrent.futures.as_completed(futures):
            name, disp_str, raw_val, raw_pct = fut.result()
            # 隐藏 QQQ，不让它占用全局宏观显示位，只留给下面特定基金调用
            if name != "纳指ETF(QQQ)" and disp_str != "纳指ETF(QQQ):暂无":
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
# 3. 组装情报 (生成微信排版底稿 + 提取纯净数据字典)
# ==========================================
def collect_v4_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    macro_str, macro_raw_dict = get_macro_waterlevel()
    hard_data_dict = {}  # 核心：存储每只基金的纯硬数据字符串
    ai_prompt_etfs, ai_prompt_rules = [], []
    portfolio_raw_list = [] 
    
    idx_share = get_idx("持有份额")
    idx_eod_vol = get_idx("[EOD]昨成交额")
    idx_camp = get_idx("资产阵营")
    
    # 提取 QDII 需要的宏观数据
    us_qqq_pct = macro_raw_dict.get("纳指ETF(QQQ)", {}).get("pct")
    us_nq_pct = macro_raw_dict.get("纳指(NQ)", {}).get("pct")
    
    for row in dash_data[1:]:
        if not row or (get_idx("基金代码") != -1 and not row[get_idx("基金代码")].strip().isdigit()): continue
        
        name = row[get_idx("基金名称")]
        proxy = row[get_idx("替身代码 (ETF)")]
        rule_limit = row[get_idx("定量证伪底线")]
        rule_logic = row[get_idx("定性持仓逻辑")]
        camp = row[idx_camp].strip() if idx_camp != -1 else ""
        
        shares_str = row[idx_share].strip() if idx_share != -1 else "0"
        eod_vol_str = row[idx_eod_vol].strip() if idx_eod_vol != -1 else ""
        try: shares = float(shares_str.replace(",", ""))
        except: shares = 0.0
            
        pos_status = "【已空仓】" if shares <= 0 else "【持仓中】"
        ma20_eod = float(row[get_idx("[EOD]MA20点位")]) if get_idx("[EOD]MA20点位") != -1 and row[get_idx("[EOD]MA20点位")] else 0.0
        
        current_price, today_pct, vol_str, raw_vol_ratio = get_realtime_data(proxy, eod_vol_str)
        
        market_value = (shares * current_price) if current_price else 0.0
        mv_str = f"¥{market_value/1000:.1f}k" if market_value > 0 else "空仓"
        
        raw_ma_dist = None
        
        # === 核心改版：区分 QDII 与 A股 的展示逻辑 ===
        if "美股" in camp or "QDII" in camp or "纳斯达克" in name:
            q_str = f"{us_qqq_pct:+.2f}%" if us_qqq_pct is not None else "未知"
            n_str = f"{us_nq_pct:+.2f}%" if us_nq_pct is not None else "未知"
            hard_data = f"昨夜 {q_str} | 期指 {n_str} | 仓:{mv_str} |"
            ai_prompt_etfs.append(f"* **{name}** {pos_status}: 昨夜涨跌 {q_str} | 盘前(期指) {n_str} | 底线纪律:{rule_limit}")
        else:
            ma_status = "MA20:未知"
            if current_price and ma20_eod > 0:
                raw_ma_dist = ((current_price - ma20_eod) / ma20_eod) * 100
                ma_status = f"MA20乖离:{raw_ma_dist:+.2f}%"
            pct_str = f"{today_pct:+.2f}%" if today_pct is not None else "停牌"
            hard_data = f"{pct_str} | 仓:{mv_str} | {vol_str} | {ma_status} |"
            ai_prompt_etfs.append(f"* **{name}**({proxy}) {pos_status}: 今日 {pct_str} | {ma_status} | 底线纪律:{rule_limit}")
        
        if shares > 0 or today_pct is not None:
            hard_data_dict[name] = hard_data
            
        ai_prompt_rules.append(f"- **{name}**: {rule_logic}")
        portfolio_raw_list.append({
            "name": name,
            "status": "空仓" if shares <= 0 else "持仓",
            "market_value_k": round(market_value/1000, 2),
            "pct_change": today_pct,
            "vol_ratio": round(raw_vol_ratio, 2) if raw_vol_ratio else None,
            "ma20_divergence_pct": round(raw_ma_dist, 2) if raw_ma_dist else None
        })

    md_prompt = f"## 🎯 场内盘口状态\n{chr(10).join(ai_prompt_etfs)}\n## 🌍 宏观水位\n{macro_str}"
    
    return md_prompt, "\n".join(ai_prompt_rules), macro_str, hard_data_dict, macro_raw_dict, portfolio_raw_list

# ==========================================
# 4. AI 首席风控官 (严格限制 JSON 字典输出)
# ==========================================
def ask_v4_tactical_agent(md_prompt: str, rules_str: str) -> dict:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: 顶级对冲基金量化总监
当前是 14:45。请基于盘口和宏观数据输出冷静专业的决断。

【输入盘口情报】：
{md_prompt}
【定性逻辑参考】：
{rules_str}

# 🔴 核心约束条件：
1. 必须为输入的【每一个标的】（无论是否空仓）在 `fund_decisions` 中输出决策动作和理由，绝不可遗漏！
2. 状态判定：如果标的标注了“【已空仓】”，绝对不能喊“卖出/清仓”。
3. 不要使用情绪化词汇。
4. `doc_full_report` 必须长达800字，且**必须结合韩国股市(KOSPI)映射、比特币流动性水位、科技巨头资本开支流向**进行深度宏观穿透分析。

# 输出要求 (必须是合法的 JSON 格式)：
{{
  "ai_summary": "用两句话总结今天的宏观状态和盘面异常点。",
  "fund_decisions": {{
    "永赢半导体": {{
      "action": "锁仓",
      "reason": "稳居MA20之上，利润垫丰厚，红盘严禁加仓，死拿不动。"
    }},
    "华宝纳斯达克": {{
      "action": "定投",
      "reason": "未见期指-2%实质性暴跌，维持日常定投节奏。"
    }}
  }},
  "doc_full_report": "### 🌍 宏观诊断与全球阵地推演\\n(此处写入深度分析...)"
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
# 6. 企业微信终极排版拼接 (Python完美缝合)
# ==========================================
def notify_wechat(macro_str, hard_data_dict, ai_summary, fund_decisions, doc_link):
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if not robot_key: return

    orders_list = []
    # 遍历我们抓取的所有硬数据基金
    for fund_name, hard_str in hard_data_dict.items():
        # 获取 AI 的决策，如果 AI 漏了，给个默认值
        decision = fund_decisions.get(fund_name, {"action": "观望", "reason": "维持既定策略。"})
        act = decision.get("action", "观望")
        rsn = decision.get("reason", "")
        
        # 智能匹配警报 Emoji
        icon = "🟢"
        if any(x in act for x in ["清仓", "止损", "卖出", "减仓"]): icon = "🚨"
        elif any(x in act for x in ["买入", "加仓", "建仓", "定投"]): icon = "🔥"
        
        # 终极缝合：状态图标 + 名称 + 动作 + 硬数据列阵 + AI理由
        orders_list.append(f"{icon} **{fund_name}**：{act} | {hard_str} {rsn}")
    
    orders_block = "\n".join([f"- {o}" for o in orders_list])
    
    content = f"""🚀 **V4.0 战术中枢 | 14:45 全阵地快照**

🌍 **全球水位**:
`{macro_str}`

🧠 **AI 首席决断**:
> {ai_summary}

⚡ **全阵地扫描与指令**:
{orders_block}

🔗 **[点击查阅深度穿透研报]({doc_link})**"""

    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    try:
        requests.post(f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}", json=payload)
    except: pass

if __name__ == "__main__":
    MY_DOC_ID = "1ydm84CsKPnM3uFB4iSJsQrJ2A-sHV38GCt9_KMRV4vY"
    
    creds = get_google_credentials()
    gc = gspread.authorize(creds)
    
    md_prompt, rules_str, macro_str, hard_data_dict, macro_raw_dict, portfolio_raw_list = collect_v4_intelligence(gc)
    ai_json = ask_v4_tactical_agent(md_prompt, rules_str)
    
    doc_link = update_google_doc(creds, ai_json["doc_full_report"], MY_DOC_ID)
    notify_wechat(macro_str, hard_data_dict, ai_json["ai_summary"], ai_json.get("fund_decisions", {}), doc_link)
    
    archive_json = {
        "timestamp": datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        "state_macro": macro_raw_dict,
        "state_portfolio": portfolio_raw_list,
        "action_ai_decision": ai_json
    }
    
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/AI_Trade_Log_{datetime.datetime.now().strftime('%Y%m%d')}.json", "w", encoding="utf-8") as f:
        json.dump(archive_json, f, ensure_ascii=False, indent=2)
