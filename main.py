import os
import json
import datetime
import pytz
import gspread
import akshare as ak
import yfinance as yf
from google import genai

# ==========================================
# 认证初始化：全局复用 Gspread 客户端
# ==========================================
def get_gspread_client():
    """解析 GCP 凭证并返回 gspread 客户端"""
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("环境变量中缺失 GCP_SERVICE_ACCOUNT")
    creds_dict = json.loads(creds_json)
    return gspread.service_account_from_dict(creds_dict)

# ==========================================
# 模块 1-A：抓取当前持仓表 (Portfolio Ingestion)
# ==========================================
def get_portfolio_data(gc) -> dict:
    """
    基于你的《基金净值总结》表格，动态抓取当前的持仓与盈亏状态
    """
    sh = gc.open("Fund_Dashboard")
    try:
        worksheet = sh.worksheet("基金净值总结")
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError("未找到名为 '基金净值总结' 的工作表，请检查表格名称。")
        
    data = worksheet.get_all_values()
    if not data:
        return {}

    headers = data[0]
    
    # 动态获取列索引，防止你在表里增加列导致报错
    def get_col_idx(keyword):
        for i, h in enumerate(headers):
            if keyword in h: return i
        return -1

    idx_name = get_col_idx("基金名称")
    idx_today_pl = get_col_idx("今日总盈亏") # 表头包含了总盈亏数据
    idx_total_pl = get_col_idx("累计总盈亏") # 表头包含了总盈亏数据
    idx_proxy_name = get_col_idx("替身代码")
    idx_proxy_price = get_col_idx("实时价格")
    idx_proxy_change = get_col_idx("实时涨跌幅")

    # 1. 从表头提取今日大盘总盈亏（如："今日总盈亏\n-¥374.70"）
    overall_today = headers[idx_today_pl].replace('\n', ' ') if idx_today_pl != -1 else "N/A"
    overall_total = headers[idx_total_pl].replace('\n', ' ') if idx_total_pl != -1 else "N/A"

    # 2. 遍历提取每个基金的表现
    funds = []
    for row in data[1:]:
        # 基金代码列如果为空或不是数字，说明是空行或注释，跳过
        if not row or len(row) == 0 or not str(row[0]).strip().isdigit():
            continue
            
        def safe_get(idx):
            return row[idx] if idx != -1 and idx < len(row) else ""

        fund_info = {
            "基金名称": safe_get(idx_name),
            "今日盈亏": safe_get(idx_today_pl),
            "累计盈亏": safe_get(idx_total_pl),
            "ETF替身": safe_get(idx_proxy_name),
            "替身实时状态": safe_get(idx_proxy_price),
            "替身实时涨跌": safe_get(idx_proxy_change)
        }
        funds.append(fund_info)

    return {
        "portfolio_summary": {
            "账户今日表现": overall_today,
            "账户累计表现": overall_total
        },
        "holdings_detail": funds
    }

# ==========================================
# 模块 1-B：抓取宏观数据 (Macro Data)
# ==========================================
def get_macro_market_data() -> dict:
    """抓取全球宏观数据作为背景辅助分析"""
    tz_bj = pytz.timezone('Asia/Shanghai')
    market_data = {"system_time": datetime.datetime.now(tz_bj).strftime('%Y-%m-%d %H:%M:%S')}

    try:
        # 韩国 KOSPI 指数 (^KS11)
        kospi = yf.Ticker("^KS11").history(period="2d")
        if len(kospi) >= 2:
            pct_change = ((kospi['Close'].iloc[-1] - kospi['Close'].iloc[-2]) / kospi['Close'].iloc[-2]) * 100
            market_data["KOSPI_change"] = f"{pct_change:.2f}%"

        # 比特币最新价格 (BTC-USD)
        btc = yf.Ticker("BTC-USD").history(period="1d")
        if not btc.empty:
            market_data["BTC_price"] = f"${btc['Close'].iloc[-1]:.2f}"

        # 美国10年期国债收益率 (^TNX)
        us10y = yf.Ticker("^TNX").history(period="1d")
        if not us10y.empty:
            market_data["US10Y_Yield"] = f"{us10y['Close'].iloc[-1]:.3f}%"
    except Exception as e:
        print(f"⚠️ 宏观数据抓取轻微异常: {e}")

    return market_data

# ==========================================
# 模块 2：AI 调用 (The Brain)
# ==========================================
def ask_fund_agent(macro_data: dict, portfolio_data: dict) -> str:
    """
    使用官方最新 google-genai 库，将持仓情况发给 Gemini
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("环境变量中缺失 GEMINI_API_KEY")

    client = genai.Client(api_key=api_key)
    
    # 组装发给 AI 的完整上下文 JSON
    ai_payload = json.dumps({
        "macro_background": macro_data,
        "my_portfolio_status": portfolio_data
    }, ensure_ascii=False, indent=2)
    
    prompt = f"""
    你是一位顶级的量化金融分析师和基金经理。
    下面是最新的【全球宏观数据】以及我当前的【实际基金持仓表（含各个基金当日盈亏与ETF替身涨跌）】。
    
    【数据源】
    {ai_payload}
    
    【你的任务】
    请用 Markdown 格式输出一段清晰、专业的今日投资决策日志。必须包含：
    1. **宏观速览**：一句话点评当天的宏观背景（BTC、美债、KOSPI等）。
    2. **账户体检**：点评我账户的今日整体盈亏表现。
    3. **个基诊断与操作建议**：找出我持仓表格中表现最好或最差（或者替身ETF出现异动）的核心基金（如：半导体、A500、黄金、机器人等），逐一给出具体的建议（如：继续持有、逢低加仓、考虑止盈等）。
    
    请直接输出分析结果，语言简明扼要，像华尔街内部的 Daily Briefing。
    """

    response = client.models.generate_content(
        model='gemini-3.1-pro-preview',
        contents=prompt
    )
    return response.text

# ==========================================
# 模块 3：写入 Google Sheets (The Hand)
# ==========================================
def update_google_sheet(gc, ai_decision_text: str):
    """
    将 AI 决策结果追加到 'AI_Daily_Log' 工作表
    """
    sh = gc.open("Fund_Dashboard")
    
    # 如果不存在日志表，则动态创建它以防报错
    try:
        worksheet = sh.worksheet("AI_Daily_Log")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="AI_Daily_Log", rows="1000", cols="5")
        worksheet.append_row(["日期", "AI 决策与点评"])
    
    tz_bj = pytz.timezone('Asia/Shanghai')
    today_str = datetime.datetime.now(tz_bj).strftime('%Y-%m-%d %H:%M')
    
    worksheet.append_row([today_str, ai_decision_text])

# ==========================================
# 主函数组合 (The Workflow)
# ==========================================
if __name__ == "__main__":
    print("🚀 [Workflow Start] 启动每日 AI 基金决策引擎...")
    
    try:
        # 初始化 Google 表格客户端
        gc = get_gspread_client()

        # Step 1: 获取数据
        print("⏳ [1/3] 正在获取实际持仓状况与宏观数据...")
        portfolio_data = get_portfolio_data(gc)
        macro_data = get_macro_market_data()
        print("✅ 数据获取成功！")
        print(">> 今日账户概览:", portfolio_data.get("portfolio_summary", {}))
        
        # Step 2: AI 分析
        print("\n⏳ [2/3] 正在呼叫 Gemini-3.1-pro 大脑进行诊断...")
        ai_response = ask_fund_agent(macro_data, portfolio_data)
        print("✅ AI 思考完毕！内容预览：")
        print("-" * 40)
        print(ai_response[:200] + "\n......[内容截断]")
        print("-" * 40)
        
        # Step 3: 写入日志
        print("\n⏳ [3/3] 正在将决策写入 AI_Daily_Log...")
        update_google_sheet(gc, ai_response)
        print("✅ 写入成功！表格已更新。")
        
        print("\n🎉 [Workflow Success] 今日自动化诊断任务圆满完成！")
        
    except Exception as e:
        print(f"\n❌ [Workflow Failed] 任务执行失败，错误信息: {e}")
        raise e
