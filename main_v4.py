import os
import json
import datetime
import pytz
import gspread
import requests
import yfinance as yf
import concurrent.futures
from google.oauth2.service_account import Credentials
from google import genai

# ==========================================
# 0. 基础辅助函数与鉴权
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
    if not creds_json: raise ValueError("缺失 GCP_SERVICE_ACCOUNT")
    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/documents"
    ]
    return Credentials.from_service_account_info(creds_dict, scopes=scopes)

# ==========================================
# 1. 宏观水位引擎 (V5.0 抗断联+双路汇率版)
# ==========================================
def get_macro_waterlevel():
    tickers = {
        "美债(US10Y)": "^TNX",
        "BTC": "BTC-USD",
        "KOSPI": "^KS11",
        "黄金(XAU)": "GC=F",
        "纳指(NQ)": "NQ=F",
        "恐慌指数(VIX)": "^VIX",
        "离岸人民币(USD/CNH)": "USDCNH=X",
        "XBI": "XBI"
    }
    macro_strs = []
    macro_raw_dict = {} 
    
    def fetch_ticker(name, symbol):
        # 🛡️ 护甲1：拉长雅虎抓取窗口至 5 天，彻底防止时区和周末断层
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="5d") 
            if not hist.empty and len(hist) >= 2:
                close = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2]
                pct = ((close - prev) / prev) * 100
                
                if name in ["BTC", "黄金(XAU)", "纳指(NQ)"]: val_str = f"${int(close):,}"
                elif name in ["美债(US10Y)", "恐慌指数(VIX)"]: val_str = f"{close:.2f}"
                elif name == "离岸人民币(USD/CNH)": val_str = f"¥{close:.4f}"
                else: val_str = f"{int(close)}"
                return name, f"{name}:{val_str}({pct:+.2f}%)", round(close, 4), round(pct, 2)
        except: pass
            
        # 🛡️ 护甲2：如果雅虎的汇率被墙，瞬间切到新浪外汇专线！
        if symbol == "USDCNH=X":
            try:
                resp = requests.get("https://hq.sinajs.cn/list=fx_susdcnh", headers={"Referer": "https://finance.sina.com.cn"}, timeout=3)
                data = resp.text.split(',')
                if len(data) > 8:
                    now_p, prev_p = float(data[1]), float(data[3])
                    pct = ((now_p - prev_p) / prev_p) * 100
                    return name, f"{name}:¥{now_p:.4f}({pct:+.2f}%)", round(now_p, 4), round(pct, 2)
            except: pass
            
        return name, f"{name}:暂无", None, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_ticker, k, v): k for k, v in tickers.items()}
        for fut in concurrent.futures.as_completed(futures):
            name, disp_str, raw_val, raw_pct = fut.result()
            macro_strs.append(disp_str)
            if raw_val is not None: macro_raw_dict[name] = {"value": raw_val, "pct": raw_pct}
            
    return " | ".join(macro_strs), macro_raw_dict

# ==========================================
# 2. 极速盘口与量比核算 (V5.0 早盘量比锁)
# ==========================================
def get_realtime_data(proxy_code: str, eod_vol_str: str):
    if not proxy_code: return None, None, "无代码", None
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
                
                if today_turnover < 50000000: return current_price, pct_change, "流动性枯竭", 1.0
                
                now_bj = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
                
                # 🛡️ 护甲3：早盘时间锁！13:30 之前绝对不计算量比，防 AI 被假放量欺骗
                if now_bj.time() < datetime.time(13, 30):
                    return current_price, pct_change, "早盘量比失真(防诱多锁)", 1.0
                
                if eod_vol_str:
                    try:
                        eod_vol = float(eod_vol_str.replace(",", ""))
                        if now_bj.time() <= datetime.time(15, 0): minutes_passed = 120 + (now_bj.hour * 60 + now_bj.minute) - (13 * 60)
                        else: minutes_passed = 240
                            
                        minutes_passed = max(1, minutes_passed)
                        raw_vol_ratio = (today_turnover / minutes_passed) / (eod_vol / 240)
                        
                        vol_tag = "平量"
                        if raw_vol_ratio > 1.2: vol_tag = "放量"
                        elif raw_vol_ratio < 0.8: vol_tag = "缩量"
                        
                        return current_price, pct_change, f"{vol_tag}({raw_vol_ratio:.2f})", raw_vol_ratio
                    except: pass
                return current_price, pct_change, "量比未知", None
    except: pass
    return None, None, "API异常", None

# ==========================================
# 3. 全阵地情报收集 (V5.0 乖离率防震板)
# ==========================================
def collect_v4_intelligence(gc):
    # 🌟 参数化 Sheet ID
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id: raise ValueError("缺失 GOOGLE_SHEET_ID 环境变量")
    sh = gc.open_by_key(sheet_id)
    
    macro_str, macro_raw_dict = get_macro_waterlevel()
    
    # 3.1 扫荡 Dashboard (持仓区)
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    portfolio_status = []
    hard_data_dict = {}
    portfolio_raw_list = []
    rules_list = []
    
    for row in dash_data[1:]:
        if not row or not any(row): continue
        fund_name = row[get_idx("基金名称")]
        proxy_raw = row[get_idx("替身代码 (ETF)")].strip() if get_idx("替身代码 (ETF)") != -1 else ""
        import re
        proxy_code = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        
        logic = row[get_idx("定性持仓逻辑")]
        bottom_line = row[get_idx("定量证伪底线")]
        cost = row[get_idx("持仓成本")]
        shares = row[get_idx("持有份额")]
        
        eod_vol = row[get_idx("[EOD]昨成交额")]
        eod_ma20_str = row[get_idx("[EOD]MA20点位")]
        eod_ma60_str = row[get_idx("[EOD]MA60点位")]
        
        rules_list.append(f"- 【{fund_name}】 定性逻辑: {logic} | 定量底线: {bottom_line}")
        
        curr_p, pct, vol_str, _ = get_realtime_data(proxy_code, eod_vol)
        
        hold_value = "空仓"
        status_tag = "🟢在持"
        if shares and cost:
            try:
                if float(shares) <= 0:
                    status_tag = "🔕已空仓"
                else:
                    hold_value = f"¥{int(float(shares) * float(cost)) / 1000:.1f}k"
            except: pass
        else: status_tag = "🔕已空仓"

        dev_str = ""
        # 🛡️ 护甲4：物理防震板，计算乖离率如果异常离谱，直接打上预警标签！
        if curr_p:
            dev_items = []
            try:
                ma20 = float(eod_ma20_str)
                dev20 = (curr_p - ma20) / ma20 * 100
                dev_items.append(f"M20:{dev20:+.2f}%" if abs(dev20) < 20 else f"M20:异常预警({dev20:+.1f}%)")
            except: pass
            try:
                ma60 = float(eod_ma60_str)
                dev60 = (curr_p - ma60) / ma60 * 100
                dev_items.append(f"M60:{dev60:+.2f}%" if abs(dev60) < 20 else f"M60:异常预警({dev60:+.1f}%)")
            except: pass
            dev_str = " | ".join(dev_items)

        pct_str = f"{pct:+.2f}%" if pct is not None else "停牌/未知"
        hard_str = f"状态:{status_tag} | 涨跌:{pct_str} | 仓:{hold_value} | {vol_str}"
        if dev_str: hard_str += f" | {dev_str}"
        
        portfolio_status.append(f"- {fund_name} ({proxy_code}): {hard_str}")
        hard_data_dict[fund_name] = hard_str
        portfolio_raw_list.append({"name": fund_name, "proxy": proxy_code, "pct": pct, "vol": vol_str})

    # 3.2 扫荡 雷达监控表
    radar_status = []
    try:
        ws_radar = sh.worksheet("雷达监控")
        radar_data = ws_radar.get_all_values()
        r_headers = radar_data[0]
        
        for row in radar_data[1:]:
            if not row or not any(row): continue
            r_name = row[r_headers.index("板块名称")]
            r_proxy_raw = row[r_headers.index("替身代码")].strip()
            r_proxy_code = re.search(r'\d{6}', r_proxy_raw).group(0) if re.search(r'\d{6}', r_proxy_raw) else ""
            trigger = row[r_headers.index("🎯 V2.0 狙击触发条件 (定量扳机)")]
            r_ma20_str = row[r_headers.index("[EOD]MA20点位")]
            r_ma60_str = row[r_headers.index("[EOD]MA60点位")]
            
            curr_p, pct, vol_str, _ = get_realtime_data(r_proxy_code, "")
            dev_str = ""
            if curr_p:
                dev_items = []
                try:
                    ma60 = float(r_ma60_str)
                    dev60 = (curr_p - ma60) / ma60 * 100
                    dev_items.append(f"M60乖离:{dev60:+.2f}%" if abs(dev60) < 20 else f"M60:异常")
                except: pass
                dev_str = " | ".join(dev_items)
            
            pct_str = f"{pct:+.2f}%" if pct is not None else "未知"
            radar_status.append(f"- 【雷达】{r_name}: {pct_str} | {dev_str} | 🎯扳机: {trigger}")
    except Exception as e:
        print(f"雷达扫描异常: {e}")

    prompt_md = f"【宏观水位】\n{macro_str}\n\n【场内盘口状态 (持仓区)】\n" + "\n".join(portfolio_status) + "\n\n【雷达监控池】\n" + "\n".join(radar_status)
    rules_str = "\n".join(rules_list)
    return prompt_md, rules_str, macro_str, hard_data_dict, macro_raw_dict, portfolio_raw_list

# ==========================================
# 4. AI 首席风控官 (V5.0 纪律约束版)
# ==========================================
def ask_v4_tactical_agent(md_prompt: str, rules_str: str) -> dict:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: 顶级对冲基金量化总监 (FundAgent V5.0)
当前是 14:45（A股尾盘15分钟，美股盘前）。请基于盘口硬数据和宏观水位，严格执行纪律，输出冷酷、专业的决断。

【输入盘口情报】：
{md_prompt}
【Google Sheet 宪法级纪律库】：
{rules_str}

# 🔴 核心风控约束条件（违反即视为系统崩溃与失职）：
1. **VIX 物理熔断**：若宏观数据中恐慌指数(VIX) > 25，触发【一级防御状态】。此时无论标的跌得多深，绝对禁止“左侧买入/加仓”，全部强制为“锁仓观望/放弃接飞刀”。
2. **汇率测谎仪**：若 A 股宽基红盘放量，但离岸人民币大幅贬值，必须在 reason 中指出“汇率背离，警惕诱多，禁止追涨”。
3. **早盘防诱多锁**：若盘口数据中带有 `早盘量比失真` 标签，说明当前是早间混沌期，绝对禁止以“量比放大”为理由下达追高买入指令！
4. **⚠️ 均线防噪法则**：任何资产的 MA20/MA60 乖离率在 [-1.5%, +1.5%] 区间内，统称为“震荡区噪音”。绝对禁止仅凭此微小乖离率下达清仓指令。
5. **铁血判决权**：必须严格比对《宪法级纪律库》中的 `[定量证伪底线]`。若未完全满足条件，一律执行“锁仓观望”，严禁主观恐慌斩仓！
6. **空仓限制**：状态为“【已空仓】”的标的，绝不可喊“卖出/清仓”。
7. **📡 雷达池双轨汇报**：严格比对【雷达监控池】的【🎯扳机】条件。如果有标的触发扳机，必须在 `radar_signals` 中输出短警报；同时在 `doc_radar_analysis` 中对雷达池内所有标的进行深度解析。

# 输出要求 (必须是合法、可解析的 JSON 格式)：
绝不允许在输出中包含 ```json 标签或任何 Markdown 格式，必须直接以 {{ 开头，}} 结尾！
{{
  "ai_summary": "用两句话总结今日宏观状态（VIX/汇率/BTC）和盘面异常资金动作。",
  "fund_decisions": {{
    "某某基金": {{
      "action": "动作", // 必须五选一：["锁仓观望", "左侧狙击", "定投维持", "清仓出局", "静默(已空仓)"]
      "reason": "严格结合 [定量证伪底线] 校验结果与宏观熔断逻辑的执行理由（限30字以内）。"
    }}
  }},
  "radar_signals": [
    {{
      "name": "有色金属",
      "signal": "🔥 触发首仓扳机",
      "reason": "已缩量回踩20日线，符合狙击条件，建议动用备用金进场。"
    }}
  ],
  "doc_radar_analysis": "### 📡 雷达池深度扫描报告\\n(对雷达池内所有标的当前均线状态、宏观相关性进行分析，不少于200字。)",
  "doc_full_report": "### 🌍 宏观诊断与全球阵地推演\\n(此处写入深度分析，必须长达800字，强制结合 VIX 恐慌情绪、人民币汇率暗流进行推演。)"
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
# 5. 通知渠道 (保留您的微信/Doc推流逻辑)
# ==========================================
def update_google_doc(creds, content: str, document_id: str):
    try:
        from googleapiclient.discovery import build
        docs_service = build('docs', 'v1', credentials=creds)
        requests_body = [{'insertText': {'location': {'index': 1}, 'text': content + "\n\n" + "="*40 + "\n\n"}}]
        docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests_body}).execute()
        return f"[https://docs.google.com/document/d/](https://docs.google.com/document/d/){document_id}/edit"
    except Exception as e:
        print(f"Doc更新失败: {e}")
        return ""

def notify_wechat(macro_str, ai_summary, orders_block, doc_link):
    webhook = os.environ.get("WECHAT_WEBHOOK")
    if not webhook: return
    content = f"【🏦 FundAgent V5.0 午盘风控】\n\n🌍 宏观水位:\n{macro_str}\n\n🧠 AI 首席决断:\n{ai_summary}\n\n⚡ 阵地扫描与指令:\n{orders_block}\n\n📝 深度研报:\n{doc_link}"
    requests.post(webhook, json={"msgtype": "text", "text": {"content": content}})

# ==========================================
# 6. 终极组装与起爆
# ==========================================
if __name__ == "__main__":
    # 🌟 参数化 Doc ID
    MY_DOC_ID = os.environ.get("GOOGLE_DOC_ID")
    if not MY_DOC_ID: raise ValueError("❌ 缺失环境变量 GOOGLE_DOC_ID！请检查 Github Secrets 配置。")
    
    creds = get_google_credentials()
    gc = gspread.authorize(creds)
    
    md_prompt, rules_str, macro_str, hard_data_dict, macro_raw_dict, portfolio_raw_list = collect_v4_intelligence(gc)
    ai_json = ask_v4_tactical_agent(md_prompt, rules_str)
    
    # --- 1. 解析常规持仓指令 ---
    orders_list = []
    fund_decisions = ai_json.get("fund_decisions", {})
    for fund_name, hard_str in hard_data_dict.items():
        decision = fund_decisions.get(fund_name, {"action": "锁仓观望", "reason": "无指令回传。"})
        act = decision.get("action", "锁仓观望")
        rsn = decision.get("reason", "")
        
        icon = "🟢"
        if any(x in act for x in ["清仓", "止损出局"]): icon = "🚨"
        elif any(x in act for x in ["狙击", "定投", "左侧"]): icon = "🔥"
        elif "静默" in act: icon = "🔕"
        
        orders_list.append(f"{icon} **{fund_name}**：{act} | {hard_str} {rsn}")
    
    orders_block = "\n".join([f"- {o}" for o in orders_list])
    
    # --- 2. 解析雷达狙击警报 (给微信短消息) ---
    radar_signals = ai_json.get("radar_signals", [])
    if radar_signals:
        radar_list = []
        for rs in radar_signals:
            radar_list.append(f"🎯 **{rs.get('name')}**: {rs.get('signal')} | {rs.get('reason')}")
        wechat_radar_block = "\n\n【📡 雷达备用金狩猎区】\n" + "\n".join([f"- {r}" for r in radar_list])
    else:
        wechat_radar_block = "\n\n【📡 雷达备用金狩猎区】\n- 🟢 暂无标的触发绝杀扳机，继续耐心潜伏。"

    ai_summary = ai_json.get("ai_summary", "")
    
    # --- 3. 提取长篇深度研报 (给 Google Docs) ---
    doc_radar_analysis = ai_json.get("doc_radar_analysis", "暂无雷达深度分析。")
    doc_full_report = ai_json.get("doc_full_report", "暂无宏观分析。")
    
    full_doc_body = f"""【🌍 全球水位】
{macro_str}

【🧠 AI 首席决断】
{ai_summary}

【⚡ 核心持仓扫描与指令 (盘口硬数据)】
{orders_block}

{doc_radar_analysis}

{doc_full_report}
"""
    
    doc_link = update_google_doc(creds, full_doc_body, MY_DOC_ID)
    notify_wechat(macro_str, ai_summary, orders_block + wechat_radar_block, doc_link)
    
    print("✅ V5.0 昼间空战全流程执行完毕！")
