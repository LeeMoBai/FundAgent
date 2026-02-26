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
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/documents"]
    return Credentials.from_service_account_info(creds_dict, scopes=scopes)

# ==========================================
# 1. 宏观水位引擎 (恢复纳指 QQQ 数据抓取)
# ==========================================
def get_macro_waterlevel():
    tickers = {
        "美债(US10Y)": "^TNX", "BTC": "BTC-USD", "KOSPI": "^KS11",
        "黄金(XAU)": "GC=F", "纳指(NQ)": "NQ=F", "纳指ETF(QQQ)": "QQQ", 
        "恐慌指数(VIX)": "^VIX", "离岸人民币(USD/CNH)": "USDCNH=X", "XBI": "XBI"
    }
    macro_strs = []
    macro_raw_dict = {} 
    
    def fetch_ticker(name, symbol):
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

    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as executor:
        futures = {executor.submit(fetch_ticker, k, v): k for k, v in tickers.items()}
        for fut in concurrent.futures.as_completed(futures):
            name, disp_str, raw_val, raw_pct = fut.result()
            # 隐藏 QQQ 不在宏观总览里刷屏，但保留其底层数据供后续调用
            if name != "纳指ETF(QQQ)" and "暂无" not in disp_str:
                macro_strs.append(disp_str)
            if raw_val is not None: macro_raw_dict[name] = {"value": raw_val, "pct": raw_pct}
    return " | ".join(macro_strs), macro_raw_dict

# ==========================================
# 2. 极速盘口与量比核算 (V4.1 双发引擎防误报版)
# ==========================================
def get_realtime_data(proxy_code: str, eod_vol_str: str):
    if not proxy_code: return None, None, "无代码", None
    
    prefix = "sh" if proxy_code.startswith("5") else "sz"
    current_price, pct_change, today_turnover = None, None, None
    
    # 🛡️ 引擎 A：首选腾讯极速 API
    try:
        url_tx = f"http://qt.gtimg.cn/q={prefix}{proxy_code}"
        resp = requests.get(url_tx, timeout=3)
        data = resp.text.split('~')
        if len(data) > 40:
            current_price = float(data[3])
            pct_change = float(data[32])
            today_turnover = float(data[37]) * 10000 # 腾讯单位是万元，转为元
    except: pass

    # 🛡️ 引擎 B：如果腾讯宕机/超时，瞬间切至新浪财经专线！
    if current_price is None:
        try:
            url_sina = f"https://hq.sinajs.cn/list={prefix}{proxy_code}"
            headers = {"Referer": "https://finance.sina.com.cn"}
            resp = requests.get(url_sina, headers=headers, timeout=3)
            elements = resp.text.split(',')
            if len(elements) > 10:
                prev_close = float(elements[2])
                current_price = float(elements[3])
                if prev_close > 0: pct_change = (current_price - prev_close) / prev_close * 100
                else: pct_change = 0.0
                today_turnover = float(elements[9]) # 新浪单位直接是元
        except: pass

    # 🚨 双引擎全部熄火才判定为异常
    if current_price is None:
        return None, None, "API异常", None

    try:
        # 量比防诱多与枯竭逻辑
        if today_turnover < 50000000: return current_price, pct_change, "枯竭", 1.0
        
        now_bj = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
        if now_bj.time() < datetime.time(13, 30): return current_price, pct_change, "早盘失真", 1.0
        
        if eod_vol_str:
            eod_vol = float(str(eod_vol_str).replace(",", ""))
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
# ==========================================
# 3. 全阵地情报收集 (V5.1 微信极简瘦身版)
# ==========================================
def collect_v4_intelligence(gc):
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id: raise ValueError("缺失 GOOGLE_SHEET_ID 环境变量")
    sh = gc.open_by_key(sheet_id)
    macro_str, macro_raw_dict = get_macro_waterlevel()
    
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    portfolio_status, hard_data_dict, portfolio_raw_list, rules_list = [], {}, [], []
    
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
        nav_str = row[get_idx("最新净值")]
        eod_vol = row[get_idx("[EOD]昨成交额")]
        eod_ma20_str = row[get_idx("[EOD]MA20点位")]
        eod_ma60_str = row[get_idx("[EOD]MA60点位")]
        
        # 🛡️ 严格保持 8 个空格的缩进
        rules_list.append(f"- 【{fund_name}】 定性逻辑: {logic} | 定量底线: {bottom_line}")
        
        hold_value = "空仓"
        status_tag = "在持"
        if shares:
            try:
                if float(shares) <= 0: 
                    status_tag = "已空仓"
                else: 
                    price_to_calc = float(nav_str) if nav_str else float(cost)
                    hold_value = f"¥{int(float(shares) * price_to_calc) / 1000:.1f}k"
            except: pass
        else: 
            status_tag = "已空仓"

        # 🛡️ 极简 QDII 特判：如果是美股，不看A股盘中虚假走势，直接看外盘！
        if "纳斯达克" in fund_name or "标普" in fund_name:
            qqq_pct = macro_raw_dict.get("纳指ETF(QQQ)", {}).get("pct")
            nq_pct = macro_raw_dict.get("纳指(NQ)", {}).get("pct")
            qqq_str = f"{qqq_pct:+.2f}%" if qqq_pct is not None else "未知"
            nq_str = f"{nq_pct:+.2f}%" if nq_pct is not None else "未知"
            
            hard_str_compact = f"昨夜 {qqq_str} | 期指 {nq_str} | 仓:{hold_value}"
            portfolio_status.append(f"- {fund_name} ({proxy_code}): 状态:{status_tag} | {hard_str_compact}")
            hard_data_dict[fund_name] = hard_str_compact
            portfolio_raw_list.append({"name": fund_name, "proxy": proxy_code, "pct": nq_pct, "vol": "QDII无量比"})
            continue # 🚀 直接跳过，不往下走 A 股盘口抓取

        # === 以下是正常 A股/港股 的盘口抓取 ===
        curr_p, pct, vol_str, _ = get_realtime_data(proxy_code, eod_vol)
        
        dev_str = ""
        if curr_p:
            dev_items = []
            try:
                ma20 = float(eod_ma20_str)
                dev20 = (curr_p - ma20) / ma20 * 100
                dev_items.append(f"M20:{dev20:+.2f}%" if abs(dev20) < 20 else f"M20:异常")
            except: pass
            try:
                ma60 = float(eod_ma60_str)
                dev60 = (curr_p - ma60) / ma60 * 100
                dev_items.append(f"M60:{dev60:+.2f}%" if abs(dev60) < 20 else f"M60:异常")
            except: pass
            dev_str = " | ".join(dev_items)

        pct_str = f"{pct:+.2f}%" if pct is not None else "停牌"
        hard_str_compact = f"{pct_str} | 仓:{hold_value} | {vol_str}"
        if dev_str: hard_str_compact += f" | {dev_str}"
        
        portfolio_status.append(f"- {fund_name} ({proxy_code}): 状态:{status_tag} | {hard_str_compact}")
        hard_data_dict[fund_name] = hard_str_compact
        portfolio_raw_list.append({"name": fund_name, "proxy": proxy_code, "pct": pct, "vol": vol_str})

    # 雷达监控扫荡
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
            r_ma60_str = row[r_headers.index("[EOD]MA60点位")]
            
            curr_p, pct, vol_str, _ = get_realtime_data(r_proxy_code, "")
            dev_str = ""
            if curr_p:
                try:
                    ma60 = float(r_ma60_str)
                    dev60 = (curr_p - ma60) / ma60 * 100
                    dev_str = f"M60乖离:{dev60:+.2f}%" if abs(dev60) < 20 else f"M60:异常"
                except: pass
            pct_str = f"{pct:+.2f}%" if pct is not None else "未知"
            radar_status.append(f"- 【雷达】{r_name}: {pct_str} | {dev_str} | 🎯扳机: {trigger}")
    except Exception as e: print(f"雷达扫描异常: {e}")

    prompt_md = f"【宏观水位】\n{macro_str}\n\n【场内盘口状态 (持仓区)】\n" + "\n".join(portfolio_status) + "\n\n【雷达监控池】\n" + "\n".join(radar_status)
    return prompt_md, "\n".join(rules_list), macro_str, hard_data_dict, macro_raw_dict, portfolio_raw_list

# ==========================================
# 4. AI 首席风控官
# ==========================================
def ask_v4_tactical_agent(md_prompt: str, rules_str: str) -> dict:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: 顶级对冲基金量化总监 (FundAgent V5.1)
当前是 14:45（A股尾盘15分钟，美股盘前）。请基于盘口硬数据和宏观水位，严格执行纪律，输出冷酷、专业的决断。

【输入盘口情报】：
{md_prompt}
【Google Sheet 宪法级纪律库】：
{rules_str}

# 🔴 核心风控约束条件（违反即视为系统崩溃与失职）：
1. **VIX 物理熔断**：若宏观数据中恐慌指数(VIX) > 25，触发【一级防御状态】。此时绝对禁止“左侧买入/加仓”，全部强制为“锁仓观望”。
2. **汇率测谎仪**：若 A 股红盘放量，但离岸人民币大幅贬值，必须指出“汇率背离，警惕诱多，禁止追涨”。
3. **早盘防诱多锁**：若数据中带有 `早盘失真` 标签，绝对禁止以“量比放大”为理由下达追高指令！
4. **铁血判决权**：必须严格比对 `[定量证伪底线]`。若未满足条件，一律执行“锁仓观望”，严禁恐慌斩仓！
5. **空仓限制**：状态为“已空仓”的标的，绝不可喊“卖出/清仓”。
6. **📡 雷达池双轨汇报**：如果有雷达标的触发扳机，必须在 `radar_signals` 中输出警报；并在 `doc_radar_analysis` 中对雷达池标的进行深度解析。

# 输出要求 (必须是合法的 JSON 格式)：
{{
  "ai_summary": "两句话总结今日宏观状态与资金异常。",
  "fund_decisions": {{
    "某某基金": {{
      "action": "动作", // 必须五选一：["锁仓观望", "左侧狙击", "定投维持", "清仓出局", "静默"]
      "reason": "严格结合底线逻辑（限30字以内）。"
    }}
  }},
  "radar_signals": [
    {{
      "name": "标的", "signal": "🔥 触发首仓", "reason": "已回踩20日线，建议建仓。"
    }}
  ],
  "doc_radar_analysis": "### 📡 雷达池深度扫描报告\\n(对雷达池标的进行分析，不少于200字。)",
  "doc_full_report": "### 🌍 宏观诊断与推演\\n(深度宏观分析，必须长达800字。)"
}}
    """
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(temperature=0.1, response_mime_type="application/json")
    )
    return json.loads(response.text)

# ==========================================
# 5. 通知渠道 (V5.1 格式修复版)
# ==========================================
def update_google_doc(creds, content: str, document_id: str):
    try:
        from googleapiclient.discovery import build
        docs_service = build('docs', 'v1', credentials=creds)
        text_to_insert = content + "\n\n" + "="*40 + "\n\n"
        
        # 🛡️ 计算 UTF-16 长度 (防止带有 Emoji 时导致 Google Doc 接口崩溃)
        utf16_len = len(text_to_insert.encode('utf-16-le')) // 2
        
        requests_body = [
            {'insertText': {'location': {'index': 1}, 'text': text_to_insert}},
            # 🛡️ 强制格式刷：把插入的文字全部重置为常规正文 (NORMAL_TEXT)，避免被原来文档首行的标题格式污染
            {
                'updateParagraphStyle': {
                    'range': {'startIndex': 1, 'endIndex': 1 + utf16_len},
                    'paragraphStyle': {'namedStyleType': 'NORMAL_TEXT'},
                    'fields': 'namedStyleType'
                }
            },
            {
                'updateTextStyle': {
                    'range': {'startIndex': 1, 'endIndex': 1 + utf16_len},
                    'textStyle': {'fontSize': {'magnitude': 10, 'unit': 'PT'}, 'bold': False},
                    'fields': 'fontSize,bold'
                }
            }
        ]
        docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests_body}).execute()
        return f"https://docs.google.com/document/d/{document_id}/edit"
    except Exception as e:
        print(f"Doc更新失败: {e}")
        return ""

def notify_wechat(macro_str, ai_summary, orders_block, doc_link):
    webhook = os.environ.get("WECHAT_WEBHOOK")
    if not webhook: return
    if not webhook.startswith("http"):
        webhook = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={webhook.strip()}"
        
    content = f"【🏦 FundAgent V5.0 午盘风控】\n\n🌍 宏观水位:\n{macro_str}\n\n🧠 AI 首席决断:\n{ai_summary}\n\n⚡ 阵地扫描与指令:\n{orders_block}\n\n📝 深度研报:\n{doc_link}"
    try:
        requests.post(webhook, json={"msgtype": "text", "text": {"content": content}}, timeout=5)
    except: pass

# ==========================================
# 6. 终极组装与起爆
# ==========================================
if __name__ == "__main__":
    MY_DOC_ID = os.environ.get("GOOGLE_DOC_ID")
    if not MY_DOC_ID: raise ValueError("缺失 GOOGLE_DOC_ID")
    
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
        
        # 🛡️ 精准手术 1：强力碾碎 AI 吐出的隐形换行符
        rsn = decision.get("reason", "").replace('\n', '').strip()
        
        icon = "🟢"
        if any(x in act for x in ["清仓", "止损"]): icon = "🚨"
        elif any(x in act for x in ["狙击", "定投", "左侧"]): icon = "🔥"
        elif "静默" in act: icon = "🔕"
        
        # 🛡️ 精准手术 2：把基金名称两边的 ** 加粗符号请回来
        orders_list.append(f"{icon} **{fund_name}**：{act} | {hard_str} | {rsn}")
    
    orders_block = "\n".join([f"- {o}" for o in orders_list])
    
    radar_signals = ai_json.get("radar_signals", [])
    if radar_signals:
        radar_list = []
        for rs in radar_signals:
            radar_list.append(f"🎯 {rs.get('name')}: {rs.get('signal')} | {rs.get('reason')}")
        wechat_radar_block = "\n\n【📡 雷达狩猎区】\n" + "\n".join([f"- {r}" for r in radar_list])
    else:
        wechat_radar_block = "\n\n【📡 雷达狩猎区】\n- 🟢 暂无标的触发绝杀扳机，继续耐心潜伏。"

    # 🛡️ 下面这几行必须和上面的 else: 垂直对齐 (前面都是 4 个空格)
    ai_summary = ai_json.get("ai_summary", "")
    doc_radar_analysis = ai_json.get("doc_radar_analysis", "暂无雷达深度分析。")
    doc_full_report = ai_json.get("doc_full_report", "暂无宏观分析。")
    
    now_str = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
    
    full_doc_body = f"""【🗓️ 战报生成时间：{now_str}】
【🌍 全球水位】
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
    
    archive_json = {
        "timestamp": datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        "state_macro": macro_raw_dict,
        "state_portfolio": portfolio_raw_list,
        "action_ai_decision": ai_json
    }
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/AI_Trade_Log_{datetime.datetime.now().strftime('%Y%m%d')}.json", "w", encoding="utf-8") as f:
        json.dump(archive_json, f, ensure_ascii=False, indent=2)
