import os
import json
import datetime
import pytz
import gspread
import akshare as ak
import requests
import concurrent.futures
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
# 1. 获取极速盘口 (O(1) 乖离率计算)
# ==========================================
def get_realtime_price(proxy_code: str):
    """仅抓取腾讯极速API最新价，耗时 < 0.1秒"""
    try:
        prefix = "sh" if proxy_code.startswith("5") else "sz"
        url = f"http://qt.gtimg.cn/q={prefix}{proxy_code}"
        resp = fetch_with_timeout(requests.get, 3, url)
        if resp:
            data = resp.text.split('~')
            if len(data) > 32:
                return float(data[3]), float(data[32]) # 返回 (最新价, 涨跌幅)
    except:
        pass
    return None, None

# ==========================================
# 2. 组装空战情报 (提取昨晚沉淀的 EOD 数据)
# ==========================================
def collect_v4_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    tactical_etfs, tactical_rules = [], []
    
    # 核心持仓读取
    for row in dash_data[1:]:
        if not row or (get_idx("基金代码") != -1 and not row[get_idx("基金代码")].strip().isdigit()): continue
        
        name = row[get_idx("基金名称")]
        proxy = row[get_idx("替身代码 (ETF)")]
        rule_limit = row[get_idx("定量证伪底线")]
        rule_logic = row[get_idx("定性持仓逻辑")]
        
        # O(1) 极速读取昨晚的均线防守点位
        ma20_eod = float(row[get_idx("[EOD]MA20点位")]) if get_idx("[EOD]MA20点位") != -1 and row[get_idx("[EOD]MA20点位")] else 0.0
        
        current_price, today_pct = get_realtime_price(proxy)
        
        ma_status = "未知"
        if current_price and ma20_eod > 0:
            dist = ((current_price - ma20_eod) / ma20_eod) * 100
            ma_status = f"MA20乖离:{dist:+.2f}%"
        
        pct_str = f"{today_pct:+.2f}%" if today_pct is not None else "无数据"
        tactical_etfs.append(f"* **{name}**({proxy}): {pct_str} | {ma_status} | 底线:{rule_limit}")
        tactical_rules.append(f"- **{name}**: {rule_logic}")

    # 雷达池读取
    tactical_radar = []
    try:
        ws_radar = sh.worksheet("雷达监控")
        radar_data = ws_radar.get_all_values()
        r_headers = radar_data[0]
        def r_idx(kw): return next((i for i, h in enumerate(r_headers) if kw in h), -1)
        
        for row in radar_data[1:]:
            if not row or not any(row): continue
            r_name = row[r_idx("板块名称")]
            r_proxy = row[r_idx("替身代码")]
            r_trigger = row[r_idx("定量狙击扳机")]
            
            ma60_eod = float(row[r_idx("[EOD]MA60点位")]) if r_idx("[EOD]MA60点位") != -1 and row[r_idx("[EOD]MA60点位")] else 0.0
            current_price, today_pct = get_realtime_price(r_proxy)
            
            ma_status = "未知"
            if current_price and ma60_eod > 0:
                dist = ((current_price - ma60_eod) / ma60_eod) * 100
                ma_status = f"MA60乖离:{dist:+.2f}%"
                
            pct_str = f"{today_pct:+.2f}%" if today_pct is not None else "无数据"
            tactical_radar.append(f"* **{r_name}**: {pct_str} | {ma_status} | 扳机:{r_trigger}")
    except Exception as e:
        print(f"雷达池读取异常: {e}")

    md_prompt = f"""## 🎯 场内盘口与绝对均线状态 (基于昨夜 EOD 精准核算)
{chr(10).join(tactical_etfs)}

## 📡 雷达池 (备用金狩猎区)
{chr(10).join(tactical_radar)}
"""
    return md_prompt, "\n".join(tactical_rules)

# ==========================================
# 3. AI 首席风控官 (输出严谨 JSON)
# ==========================================
def ask_v4_tactical_agent(md_prompt: str, rules_str: str) -> dict:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V4.0 首席风控官与量化策略主脑 (Chief Risk & Strategy Officer)
当前是 14:45，你需要基于客观盘口乖离率，对照纪律底线，执行绝对理性的交易决断。

【输入盘口情报】：
{md_prompt}
【定性逻辑参考】：
{rules_str}

# 输出要求 (必须是合法的 JSON 格式，绝不允许输出其他废话)：
请输出如下结构的 JSON：
{{
  "wechat_summary": [
    "用3句话总结今天大盘、均线破位情况以及雷达触发情况。"
  ],
  "execution_orders": [
    "🟢 永赢半导体：锁仓 | 稳居 MA20 之上",
    "🚨 某某基金：清仓/减仓 | 跌破底线，强制执行"
  ],
  "doc_full_report": "### 🌍 宏观诊断与阵地推演\\n(包含资金流向、QDII映射、详细防守反击逻辑，约800字长篇研报排版，使用 Markdown)"
}}
    """
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(
            temperature=0.1,
            response_mime_type="application/json" # 强制输出 JSON
        )
    )
    return json.loads(response.text)

# ==========================================
# 4. Google Doc 知识库引擎 (固定阵地倒序插入)
# ==========================================
def update_google_doc(creds, report_text: str, target_doc_id: str) -> str:
    tz_bj = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_bj)
    date_str = now.strftime('%Y-%m-%d %H:%M')
    
    docs_service = build('docs', 'v1', credentials=creds)
    
    # 将今日报告直接插在文档最开头 (Index 1)
    insert_text = f"\n\n=================================\n📅 {date_str} 盘中风控决断\n=================================\n{report_text}\n"
    requests = [{'insertText': {'location': {'index': 1}, 'text': insert_text}}]
    
    docs_service.documents().batchUpdate(documentId=target_doc_id, body={'requests': requests}).execute()
    
    return f"https://docs.google.com/document/d/{target_doc_id}/edit"

# ==========================================
# 5. 企业微信极简推送
# ==========================================
def notify_wechat(summary_list, orders_list, doc_link):
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if not robot_key: return

    summary_str = "\n".join([f"> {s}" for s in summary_list])
    orders_str = "\n".join([f"- **{o}**" for o in orders_list])
    
    content = f"""🚀 **V4.0 首席风控官决断**

🔍 **核心快照**:
{summary_str}

📝 **执行指令**:
{orders_str}

🔗 **[点击直达战术总参谋部日记]({doc_link})**
*(已自动插入今日最新剖析)*"""

    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    try:
        requests.post(f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}", json=payload)
        print("📡 极简指令推送成功！")
    except Exception as e:
        print(f"❌ 微信推送异常: {e}")

if __name__ == "__main__":
    print("🚀 启动 V4.0 白天空战中枢...")
    
    # 👇👇👇 【总司令请注意】将您刚才复制的文档 ID 填在这里 👇👇👇
    MY_DOC_ID = "请在这里填入您的_Google_Doc_ID"
    
    creds = get_google_credentials()
    gc = gspread.authorize(creds)
    
    md_prompt, rules_str = collect_v4_intelligence(gc)
    print("🧠 正在请求首席风控官进行裁决...")
    ai_json = ask_v4_tactical_agent(md_prompt, rules_str)
    
    print("📝 正在向总参谋部日记注入最新研报...")
    doc_link = update_google_doc(creds, ai_json["doc_full_report"], MY_DOC_ID)
    
    print("📲 正在发送微信极简警报...")
    notify_wechat(ai_json["wechat_summary"], ai_json["execution_orders"], doc_link)
    
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/AI_Trade_Log_{datetime.datetime.now().strftime('%Y%m%d')}.json", "w", encoding="utf-8") as f:
        json.dump(ai_json, f, ensure_ascii=False, indent=2)
    
    print("✅ V4.0 全杀伤链执行完毕！")
