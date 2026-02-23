import os
import json
import datetime
import pytz
import gspread
import akshare as ak
import yfinance as yf
from google import genai
import pandas as pd

# ==========================================
# 0. 认证初始化
# ==========================================
def get_gspread_client():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("环境变量中缺失 GCP_SERVICE_ACCOUNT")
    return gspread.service_account_from_dict(json.loads(creds_json))

# ==========================================
# 1. 抓取全球宏观数据 (Macro & Futures)
# ==========================================
def get_macro_data() -> dict:
    macro = {}
    tickers = {
        "US10Y": "^TNX",
        "DXY": "DX-Y.NYB",
        "BTC": "BTC-USD",
        "KOSPI": "^KS11",
        "NQmain": "NQ=F" # 纳指期货
    }
    
    for key, symbol in tickers.items():
        try:
            data = yf.Ticker(symbol).history(period="2d")
            if len(data) >= 2:
                prev_close = data['Close'].iloc[-2]
                current = data['Close'].iloc[-1]
                pct_chg = ((current - prev_close) / prev_close) * 100
                
                # 针对不同资产格式化
                if key == "US10Y":
                    macro[key] = f"{current:.3f}% ({pct_chg:+.2f}%)"
                elif key == "BTC":
                    macro[key] = f"${current:,.0f} ({pct_chg:+.2f}%)"
                else:
                    macro[key] = f"{current:.2f} ({pct_chg:+.2f}%)"
            else:
                macro[key] = "N/A"
        except:
            macro[key] = "拉取失败"
            
    return macro

# ==========================================
# 2. 抓取突发新闻 (Nvidia 雷达)
# ==========================================
def get_nvda_news() -> str:
    try:
        news_list = yf.Ticker("NVDA").news
        if news_list and len(news_list) > 0:
            top_2 = news_list[:2]
            news_str = " | ".join([f"[{n['publisher']}] {n['title']}" for n in top_2])
            return news_str
    except:
        pass
    return "无重大宏观/产业黑天鹅新闻"

# ==========================================
# 3. 抓取持仓、历史与生成 V2.0 情报简报
# ==========================================
def collect_full_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tz_bj = pytz.timezone('Asia/Shanghai')
    today_time = datetime.datetime.now(tz_bj).strftime('%Y-%m-%d %H:%M')
    
    # 3.1 抓取宏观与新闻
    macro = get_macro_data()
    news_alert = get_nvda_news()
    
    # 3.2 抓取全市场 ETF 实时行情备用
    print("   [+] 正在拉取全市场 ETF 实时快照...")
    try:
        etf_spot = ak.fund_etf_spot_em()
    except:
        etf_spot = pd.DataFrame()

    # 3.3 遍历 Dashboard (当前持仓)
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_col_idx(kw):
        return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    idx_name, idx_proxy = get_col_idx("基金名称"), get_col_idx("替身代码")
    
    # 构建 ETF 表现文字
    etf_reports = []
    funds_context = []
    
    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[idx_name] if idx_name != -1 else "Unknown"
        proxy = row[idx_proxy].strip() if idx_proxy != -1 else ""
        
        if proxy and not etf_spot.empty:
            match = etf_spot[etf_spot["代码"] == proxy]
            if not match.empty:
                price = match.iloc[0]['最新价']
                pct = match.iloc[0]['涨跌幅']
                vol = match.iloc[0]['成交额'] / 100000000 # 转换为亿元
                
                # 核心逻辑：特殊坑位计算
                extra_note = ""
                if proxy == "513120" or proxy == "159567": # 创新药
                    distance = ((price - 1.33) / 1.33) * 100
                    extra_note = f" (距1.33坑位还差: {distance:+.2f}%)"
                
                report_line = f"* **{name} ({proxy})**: 现价 {price} | 涨跌幅 {pct:+.2f}% | 成交额: {vol:.2f}亿{extra_note}"
                etf_reports.append(report_line)
                funds_context.append({"名称": name, "涨跌": f"{pct:+.2f}%"})

    # 3.4 提取向内看的记忆（最近 3 天历史 + 最近 5 笔交易）
    try:
        history_data = sh.worksheet("History").get_all_values()
        recent_3_days = [r for r in history_data[-3:] if any(r)]
    except:
        recent_3_days = ["无数据"]
        
    try:
        trade_data = sh.worksheet("交易记录").get_all_values()
        recent_5_trades = [r for r in trade_data[-5:] if any(r)]
    except:
        recent_5_trades = ["无数据"]

    # ==============================
    # 🎯 拼装 V2.0 终极侦察简报
    # ==============================
    markdown_report = f"""# ⚡ V2.0 盘中侦察情报 [{today_time}]

## 🌍 1. 全球宏观水位 (Macro)
* **10年期美债 (US10Y)**: {macro.get('US10Y', 'N/A')}
* **美元指数 (DXY)**: {macro.get('DXY', 'N/A')}
* **比特币 (BTC)**: {macro.get('BTC', 'N/A')}
* **韩国KOSPI (半导体先行锚)**: {macro.get('KOSPI', 'N/A')}
* **纳指期货 (NQmain)**: {macro.get('NQmain', 'N/A')}

## 🎯 2. 核心场内替身盘中表现 (ETF Proxies)
"""
    markdown_report += "\n".join(etf_reports)
    
    markdown_report += f"""

## 📰 3. V2.0 专属雷达预警 (News/Alerts)
* **英伟达/算力链动态**: {news_alert}
"""

    account_memory_json = json.dumps({
        "近期3天账户走势": recent_3_days,
        "最近5笔真实交易记录": recent_5_trades
    }, ensure_ascii=False)

    return markdown_report, account_memory_json

# ==========================================
# 4. AI 决策中枢 (The Brain)
# ==========================================
def ask_fund_agent(markdown_report: str, account_memory_json: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)
    
    prompt = f"""
# Role Definition: V2.0 场外基金决策智能体
你是一个极度理性的量化基金决策大脑。
请严格遵守你的 V2.0 策略纪律（不追高、不窄幅止损、防锚定效应）。
结合下方的《14:30 侦察情报》和《账户真实记忆》，输出最终的决策。

### 输出格式要求：
必须包含三个 Markdown 模块：
1. 🌍 [宏观与主线诊断 (14:45)]
2. ⚔️ [V2.0 终极操作指令 (场外基金专用)]
3. 📝 [15:00 申赎执行单] (必须结合近期交易记录，防止重复加仓)

--- 
# 【外部视野】今日实时侦察情报：
{markdown_report}

# 【内部记忆】你服务的账户近期历史与交易操作：
{account_memory_json}
    """
    
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', # 依然使用稳定且额度高的 2.0-flash
        contents=prompt
    )
    return response.text

# ==========================================
# 5. 回写 Google Sheet
# ==========================================
def update_google_sheet(gc, ai_decision: str):
    sh = gc.open("基金净值总结")
    try:
        ws = sh.worksheet("AI-参考")
    except:
        ws = sh.add_worksheet("AI-参考", 1000, 5)
    
    today_str = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M')
    ws.append_row([today_str, ai_decision])

# ==========================================
# Workflow 主入口
# ==========================================
if __name__ == "__main__":
    print("🚀 [Workflow Start] 启动 V2.0 全息量化引擎...")
    try:
        gc = get_gspread_client()
        
        # 1. 抓取所有情报 (格式化简报 + 内部记忆)
        print("⏳ [1/3] 正在生成《V2.0 盘中侦察情报》与提取账户记忆...")
        md_report, memory_json = collect_full_intelligence(gc)
        
        print("\n" + "="*50)
        print("👇 你可以直接复制以下简报去网页版 Gemini 讨论 👇")
        print(md_report)
        print("="*50 + "\n")
        
        # 2. 调用 AI 大脑
        print("⏳ [2/3] 正在唤醒自动 AI Agent 进行决策...")
        decision = ask_fund_agent(md_report, memory_json)
        print("✅ AI 决策完成：\n", decision[:200], "...\n")
        
      # 3. 回写表格 (将盘中情报与 AI 决策合并写入)
        print("⏳ [3/3] 正在写入 Google Sheets [AI-参考]...")
        
        # 拼接原始数据与 AI 的分析结论，中间加一条华丽的分割线
        full_log_text = f"{md_report}\n\n{'='*40}\n\n{decision}"
        
        update_google_sheet(gc, full_log_text)
        
        print("🎉 [Success] 每日任务执行成功！")
        
    except Exception as e:
        print(f"❌ [Failed] 报错信息: {e}")
        raise e
