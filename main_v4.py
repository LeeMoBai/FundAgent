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
# 1. 宏观水位引擎
# ==========================================
def get_macro_waterlevel():
    tickers = {
        "美债(US10Y)": "^TNX",
        "BTC": "BTC-USD",
        "KOSPI": "^KS11",
        "黄金(XAU)": "GC=F",
        "纳指(NQ)": "NQ=F",
        "纳指ETF(QQQ)": "QQQ", 
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
            if name != "纳指ETF(QQQ)" and disp_str != "纳指ETF(QQQ):暂无":
                macro_strs.append(disp_str)
            if raw_val is not None:
                macro_raw_dict[name] = {"value": raw_val, "pct": raw_pct}
            
    return " | ".join(macro_strs), macro_raw_dict

# ==========================================
# 2. 极速盘口与量比核算 (修复午休Bug)
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
                        now_time = now_bj.time()
                        
                        # 精确扣除午休时间
                        if now_time < datetime.time(9, 30):
                            minutes_passed = 1
                        elif now_time <= datetime.time(11, 30):
                            minutes_passed = (now_bj.hour * 60 + now_bj.minute) - (9 * 60 + 30)
                        elif now_time <= datetime.time(13, 0):
                            minutes_passed = 120
                        elif now_time <= datetime.time(15, 0):
                            minutes_passed = 120 + (now_bj.hour * 60 + now_bj.minute) - (13 * 60)
                        else:
                            minutes_passed = 240
                            
                        minutes_passed = max(1, minutes_passed)
                        
                        raw_vol_ratio = (today_turnover / minutes_passed) / (eod_vol / 240)
                        
                        # 收紧阈值，敏感度拉满
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
# 3. 组装情报 (修复真实市值 Bug)
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
        
        # 1. 抓取 ETF 盘口
        current_price, today_pct, vol_str, raw_vol_ratio = get_realtime_data(proxy, eod_vol_str)
        
        # 2. 计算真实场外基金市值 (修复点)
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
            # 估算 QDII 盘中市值
            if market_value > 0 and us_qqq_pct is not None: market_value *= (1 + us_qqq_pct / 100)
            mv_str = f"¥{market_value/1000:.1f}k" if market_value > 0 else "空仓"
            
            q_str = f"{us_qqq_pct:+.2f}%" if us_qqq_pct is not None else "未知"
            n_str = f"{us_nq_pct:+.2f}%" if us_nq_pct is not None else "未知"
            hard_data = f"昨夜 {q_str} | 期指 {n_str} | 仓:{mv_str} |"
            ai_prompt_etfs.append(f"* **{name}** {pos_status}: 昨夜涨跌 {q_str} | 盘前(期指) {n_str} | 底线纪律:{rule_limit}")
        else:
            # 估算 A股 盘中市值
            if market_value > 0 and today_pct is not None: market_value *= (1 + today_pct / 100)
            mv_str = f"¥{market_value/1000:.1f}k" if market_value > 0 else "空仓"
            
            ma_status = "MA20:未知"
            if current_price and ma20_eod > 0:
                # 乖离率继续用 ETF 算，这是绝对正确的
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
# 4. AI 首席风控官
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
# 5. Google Doc 固定阵地注入 (支持左侧大纲导航)
# ==========================================
def update_google_doc(creds, report_text: str, target_doc_id: str) -> str:
    tz_bj = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_bj)
    date_str = now.strftime('%Y-%m-%d %H:%M')
    docs_service = build('docs', 'v1', credentials=creds)
    
    # 将标题和正文分开，以精准计算标题的长度
    title_text = f"📅 {date_str} 盘中风控决断\n"
    body_text = f"{report_text}\n\n"
    full_text = title_text + body_text
    
    # 组合 API 指令：先插入文字，再刷上标题格式
    requests = [
        {
            'insertText': {
                'location': {'index': 1},
                'text': full_text
            }
        },
        {
            'updateParagraphStyle': {
                'range': {
                    'startIndex': 1,
                    # 极其精准：只把标题部分的文字刷成 Heading 2，正文保持原样
                    'endIndex': 1 + len(title_text)
                },
                'paragraphStyle': {
                    'namedStyleType': 'HEADING_2'
                },
                'fields': 'namedStyleType'
            }
        }
    ]
    
    docs_service.documents().batchUpdate(documentId=target_doc_id, body={'requests': requests}).execute()
    return f"https://docs.google.com/document/d/{target_doc_id}/edit"
# ==========================================
# 6. 企业微信终极排版拼接
# ==========================================
def notify_wechat(macro_str, hard_data_dict, ai_summary, fund_decisions, doc_link):
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if not robot_key: return

    orders_list = []
    for fund_name, hard_str in hard_data_dict.items():
        decision = fund_decisions.get(fund_name, {"action": "观望", "reason": "维持既定策略。"})
        act = decision.get("action", "观望")
        rsn = decision.get("reason", "")
        
        icon = "🟢"
        if any(x in act for x in ["清仓", "止损", "卖出", "减仓"]): icon = "🚨"
        elif any(x in act for x in ["买入", "加仓", "建仓", "定投"]): icon = "🔥"
        
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
    MY_DOC_ID = "1ydm84CsKPnM3uFB4iSJsQrJ2A-sHV38GCt9_KMRV4vY" # <--- 别忘了换成您的ID
    
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
