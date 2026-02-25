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
# 1. 宏观水位引擎 (V4.2 加装 VIX 与 CNH 探针)
# ==========================================
def get_macro_waterlevel():
    tickers = {
        "美债(US10Y)": "^TNX",
        "BTC": "BTC-USD",
        "KOSPI": "^KS11",
        "黄金(XAU)": "GC=F",
        "纳指(NQ)": "NQ=F",
        "纳指ETF(QQQ)": "QQQ", 
        "XBI": "XBI",
        # 👇 新增：全球恐慌物理熔断器与外资测谎仪
        "恐慌指数(VIX)": "^VIX",
        "离岸人民币(USD/CNH)": "USDCNH=X"
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
                elif name in ["美债(US10Y)", "恐慌指数(VIX)"]:
                    val_str = f"{close:.2f}"
                elif name == "离岸人民币(USD/CNH)":
                    val_str = f"¥{close:.4f}"
                else:
                    val_str = f"{int(close)}"
                    
                return name, f"{name}:{val_str}({pct:+.2f}%)", round(close, 4), round(pct, 2)
        except:
            pass
        return name, f"{name}:暂无", None, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as executor:
        futures = {executor.submit(fetch_ticker, k, v): k for k, v in tickers.items()}
        for fut in concurrent.futures.as_completed(futures):
            name, disp_str, raw_val, raw_pct = fut.result()
            if name != "纳指ETF(QQQ)" and disp_str != "纳指ETF(QQQ):暂无":
                macro_strs.append(disp_str)
            if raw_val is not None:
                macro_raw_dict[name] = {"value": raw_val, "pct": raw_pct}
            
    return " | ".join(macro_strs), macro_raw_dict
# ==========================================
# 2. 极速盘口与量比核算 (V4.2 引入流动性枯竭拦截)
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
                today_turnover = float(data[37]) * 10000 # 转换为元
                
                vol_ratio_str = "量比未知"
                raw_vol_ratio = None
                
                # 👇 新增：微盘 ETF 绝对成交额防骗局过滤
                if today_turnover < 50000000: # 小于 5000 万 RMB
                    return current_price, pct_change, "流动性枯竭(忽略量比)", 1.0
                
                if eod_vol_str:
                    try:
                        eod_vol = float(eod_vol_str.replace(",", ""))
                        now_bj = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
                        now_time = now_bj.time()
                        
                        if now_time < datetime.time(9, 30): minutes_passed = 1
                        elif now_time <= datetime.time(11, 30): minutes_passed = (now_bj.hour * 60 + now_bj.minute) - (9 * 60 + 30)
                        elif now_time <= datetime.time(13, 0): minutes_passed = 120
                        elif now_time <= datetime.time(15, 0): minutes_passed = 120 + (now_bj.hour * 60 + now_bj.minute) - (13 * 60)
                        else: minutes_passed = 240
                            
                        minutes_passed = max(1, minutes_passed)
                        raw_vol_ratio = (today_turnover / minutes_passed) / (eod_vol / 240)
                        
                        vol_tag = "平量"
                        if raw_vol_ratio > 1.15: vol_tag = "放量"
                        elif raw_vol_ratio < 0.85: vol_tag = "缩量"
                        
                        vol_ratio_str = f"{vol_tag}({raw_vol_ratio:.2f})"
                    except:
                        pass
                        
                return current_price, pct_change, vol_ratio_str, raw_vol_ratio
    except:
        pass
    return None, None, "无量比", None
# ==========================================
# 3. 组装情报
# ==========================================
def collect_v4_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    macro_str, macro_raw_dict = get_macro_waterlevel()
    hard_data_dict = {}  
    ai_prompt_etfs, ai_prompt_rules = [], []
    portfolio_raw_list = [] 
    
    idx_share = get_idx("持有份额")
    idx_eod_vol = get_idx("[EOD]昨成交额")
    idx_camp = get_idx("资产阵营")
    idx_nav = get_idx("最新净值")
    idx_cost = get_idx("持仓成本")
    
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
        
        nav_str = row[idx_nav].strip() if idx_nav != -1 else ""
        cost_str = row[idx_cost].strip() if idx_cost != -1 else ""
        try: base_price = float(nav_str)
        except: base_price = 0.0
        if base_price == 0.0:
            try: base_price = float(cost_str)
            except: base_price = 0.0
            
        market_value = shares * base_price
        raw_ma_dist = None
        is_qdii = ("美股" in camp or "QDII" in camp or "纳斯达克" in name)
        
        if is_qdii:
            if market_value > 0 and us_qqq_pct is not None: market_value *= (1 + us_qqq_pct / 100)
            mv_str = f"¥{market_value/1000:.1f}k" if market_value > 0 else "空仓"
            q_str = f"{us_qqq_pct:+.2f}%" if us_qqq_pct is not None else "未知"
            n_str = f"{us_nq_pct:+.2f}%" if us_nq_pct is not None else "未知"
            hard_data = f"昨夜 {q_str} | 期指 {n_str} | 仓:{mv_str} |"
            ai_prompt_etfs.append(f"* **{name}** {pos_status}: 昨夜涨跌 {q_str} | 盘前(期指) {n_str} | 底线纪律:{rule_limit}")
        else:
            if market_value > 0 and today_pct is not None: market_value *= (1 + today_pct / 100)
            mv_str = f"¥{market_value/1000:.1f}k" if market_value > 0 else "空仓"
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
# 4. AI 首席风控官 (V4.2 植入终极物理熔断器)
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

# 🔴 核心风控约束条件（违反即视为系统崩溃）：
1. **VIX 物理熔断**：如果宏观数据中恐慌指数(VIX) > 25，触发【一级防御状态】。此时无论任何标的跌得多深，绝对禁止给出“左侧买入/加仓”指令，全部改为“观望/放弃接飞刀”。
2. **汇率测谎仪**：如果 A 股核心宽基红盘放量，但离岸人民币(USD/CNH)同步贬值（如突破7.25），必须在理由中指出“汇率背离，警惕内资诱多”。
3. **QDII 溢价警报**：对于华宝纳斯达克等跨境 ETF，如果期指大跌但盘口跌幅很小，必须在理由中提示“警惕场内高溢价陷阱，拒绝高位接盘”。
4. 必须为【每一个标的】输出 `fund_decisions`，状态为“【已空仓】”的绝对不能喊“卖出/清仓”。
5. `doc_full_report` 必须长达800字，强制结合 VIX 恐慌情绪、人民币汇率暗流、BTC 流动性进行深度机构级穿透。

# 输出要求 (必须是合法的 JSON 格式)：
{{
  "ai_summary": "用两句话总结今天的宏观状态和盘面异常点。",
  "fund_decisions": {{
    "某某基金": {{
      "action": "动作",
      "reason": "严格遵循熔断与测谎逻辑的理由。"
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
# 5. Google Doc 固定阵地注入 (支持大纲 + 注入全量数据)
# ==========================================
def update_google_doc(creds, report_text: str, target_doc_id: str) -> str:
    tz_bj = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_bj)
    date_str = now.strftime('%Y-%m-%d %H:%M')
    docs_service = build('docs', 'v1', credentials=creds)
    
    title_text = f"📅 {date_str} 盘中风控决断\n"
    body_text = f"{report_text}\n\n"
    full_text = title_text + body_text
    
    requests = [
        {'insertText': {'location': {'index': 1}, 'text': full_text}},
        {'updateParagraphStyle': {
            'range': {'startIndex': 1, 'endIndex': 1 + len(title_text)},
            'paragraphStyle': {'namedStyleType': 'HEADING_2'},
            'fields': 'namedStyleType'
        }}
    ]
    docs_service.documents().batchUpdate(documentId=target_doc_id, body={'requests': requests}).execute()
    return f"https://docs.google.com/document/d/{target_doc_id}/edit"

# ==========================================
# 6. 企业微信推送
# ==========================================
def notify_wechat(macro_str, ai_summary, orders_block, doc_link):
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if not robot_key: return

    content = f"""🚀 **V4.0 战术中枢 | 14:45 全阵地快照**

🌍 **全球水位**:
`{macro_str}`

🧠 **AI 首席决断**:
> {ai_summary}

⚡ **全阵地扫描与指令**:
{orders_block}

🔗 **[点击查阅全量数据战报日记]({doc_link})**"""

    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    try:
        requests.post(f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}", json=payload)
    except: pass

if __name__ == "__main__":
    MY_DOC_ID = "1ydm84CsKPnM3uFB4iSJsQrJ2A-sHV38GCt9_KMRV4vY" # <--- 别忘了填您的ID
    
    creds = get_google_credentials()
    gc = gspread.authorize(creds)
    
    md_prompt, rules_str, macro_str, hard_data_dict, macro_raw_dict, portfolio_raw_list = collect_v4_intelligence(gc)
    ai_json = ask_v4_tactical_agent(md_prompt, rules_str)
    
    # 核心修复点：将硬数据与 AI 分析提前组合！
    orders_list = []
    fund_decisions = ai_json.get("fund_decisions", {})
    for fund_name, hard_str in hard_data_dict.items():
        decision = fund_decisions.get(fund_name, {"action": "观望", "reason": "维持既定策略。"})
        act = decision.get("action", "观望")
        rsn = decision.get("reason", "")
        
        icon = "🟢"
        if any(x in act for x in ["清仓", "止损", "卖出", "减仓"]): icon = "🚨"
        elif any(x in act for x in ["买入", "加仓", "建仓", "定投"]): icon = "🔥"
        
        orders_list.append(f"{icon} **{fund_name}**：{act} | {hard_str} {rsn}")
    
    orders_block = "\n".join([f"- {o}" for o in orders_list])
    
    ai_summary = ai_json.get("ai_summary", "")
    doc_full_report = ai_json.get("doc_full_report", "")
    
    # 【拼装给 Google Docs 的“全家桶合影”】
    full_doc_body = f"""【🌍 全球水位】
{macro_str}

【🧠 AI 首席决断】
{ai_summary}

【⚡ 全阵地扫描与指令 (包含盘口硬数据)】
{orders_block}

【📝 深度宏观穿透研报】
{doc_full_report}
"""
    
    # 把全家桶发给 Google Doc
    doc_link = update_google_doc(creds, full_doc_body, MY_DOC_ID)
    
    # 把精简版发给微信
    notify_wechat(macro_str, ai_summary, orders_block, doc_link)
    
    archive_json = {
        "timestamp": datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        "state_macro": macro_raw_dict,
        "state_portfolio": portfolio_raw_list,
        "action_ai_decision": ai_json
    }
    
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/AI_Trade_Log_{datetime.datetime.now().strftime('%Y%m%d')}.json", "w", encoding="utf-8") as f:
        json.dump(archive_json, f, ensure_ascii=False, indent=2)
