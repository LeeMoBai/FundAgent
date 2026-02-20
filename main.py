import os
import json
import datetime
import pytz
import gspread
import akshare as ak
import yfinance as yf
from google import genai

# ==========================================
# 模块 1：数据抓取 (Data Ingestion)
# ==========================================
def get_market_data() -> dict:
    """
    抓取当日最新市场数据，包含北京时间、KOSPI、BTC、US10Y、半导体ETF(512480)
    """
    # 1. 获取北京时间
    tz_bj = pytz.timezone('Asia/Shanghai')
    bj_time = datetime.datetime.now(tz_bj)
    
    market_data = {
        "system_time": bj_time.strftime('%Y-%m-%d %H:%M:%S'),
        "KOSPI": "N/A",
        "BTC_price": "N/A",
        "US10Y": "N/A",
        "semiconductor_ETF_512480_vol": "N/A"
    }

    # 2. 抓取全球市场数据 (使用 yfinance 提高海外节点抓取稳定性)
    try:
        # 韩国 KOSPI 指数 (^KS11)
        kospi = yf.Ticker("^KS11").history(period="2d")
        if len(kospi) >= 2:
            prev_close = kospi['Close'].iloc[-2]
            current = kospi['Close'].iloc[-1]
            pct_change = ((current - prev_close) / prev_close) * 100
            market_data["KOSPI"] = f"{pct_change:.2f}%"

        # 比特币最新价格 (BTC-USD)
        btc = yf.Ticker("BTC-USD").history(period="1d")
        if not btc.empty:
            market_data["BTC_price"] = f"${btc['Close'].iloc[-1]:.2f}"

        # 美国10年期国债收益率 (^TNX)
        us10y = yf.Ticker("^TNX").history(period="1d")
        if not us10y.empty:
            market_data["US10Y"] = f"{us10y['Close'].iloc[-1]:.3f}%"
            
    except Exception as e:
        print(f"⚠️ 全球市场数据抓取出现部分异常: {e}")

    # 3. 抓取 A股 ETF 数据 (使用 akshare)
    try:
        # 获取半导体 ETF 512480 的实时/历史行情
        etf_df = ak.fund_etf_hist_em(symbol="512480", period="daily")
        if not etf_df.empty:
            latest_data = etf_df.iloc[-1]
            pct_chg = latest_data.get("涨跌幅", 0)
            vol = latest_data.get("成交量", 0)
            market_data["semiconductor_ETF_512480_vol"] = f"涨跌幅: {pct_chg}%, 成交量: {vol}手"
    except Exception as e:
        print(f"⚠️ 半导体ETF数据抓取异常: {e}")

    return market_data

# ==========================================
# 模块 2：AI 调用 (The Brain)
# ==========================================
def ask_fund_agent(json_data_str: str) -> str:
    """
    使用官方最新 google-genai 库请求 Gemini 大模型
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("环境变量中缺失 GEMINI_API_KEY")

    # 初始化最新版的 genai 客户端
    client = genai.Client(api_key=api_key)
    
    # 可以在这里补充你的 System Instructions 或者复杂的 Prompt
    prompt = f"""
    你是一个顶级的量化金融分析师和基金经理。
    请根据以下最新的市场数据 JSON，用 Markdown 格式输出一段简明扼要的今日市场点评和投资决策建议。
    
    市场数据：
    {json_data_str}
    """

    # 调用指定的模型（按照要求使用 gemini-3.1-pro-preview）
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview',
        contents=prompt
    )
    
    return response.text

# ==========================================
# 模块 3：写入 Google Sheets (The Hand)
# ==========================================
def update_google_sheet(ai_decision_text: str):
    """
    将 AI 决策结果追加到 Google Sheets 中
    """
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("环境变量中缺失 GCP_SERVICE_ACCOUNT")

    # 解析 GitHub Actions 传进来的 JSON 凭证字符串
    creds_dict = json.loads(creds_json)
    
    # 使用 gspread 官方推荐的免 oauth2client 字典认证方式
    gc = gspread.service_account_from_dict(creds_dict)
    
    # 打开表格并定位到工作表
    sh = gc.open("Fund_Dashboard")
    worksheet = sh.worksheet("AI_Daily_Log")
    
    # 获取北京时间日期作为第一列
    tz_bj = pytz.timezone('Asia/Shanghai')
    today_str = datetime.datetime.now(tz_bj).strftime('%Y-%m-%d')
    
    # 在表格最下方追加一行
    worksheet.append_row([today_str, ai_decision_text])

# ==========================================
# 主函数组合 (The Workflow)
# ==========================================
if __name__ == "__main__":
    print("🚀 [Workflow Start] 启动每日 AI 基金决策引擎...")
    
    try:
        # Step 1
        print("⏳ [1/3] 正在抓取市场数据 (Data Ingestion)...")
        market_data = get_market_data()
        market_data_json = json.dumps(market_data, ensure_ascii=False, indent=2)
        print("✅ 抓取成功！数据内容如下：")
        print(market_data_json)
        
        # Step 2
        print("\n⏳ [2/3] 正在呼叫 AI 大脑进行分析 (The Brain)...")
        ai_response = ask_fund_agent(market_data_json)
        print("✅ AI 思考完毕！内容预览：")
        print(ai_response[:150] + "......[内容截断]")
        
        # Step 3
        print("\n⏳ [3/3] 正在将决策写入 Google Sheets (The Hand)...")
        update_google_sheet(ai_response)
        print("✅ 写入成功！表格已更新。")
        
        print("\n🎉 [Workflow Success] 今日任务圆满完成！")
        
    except Exception as e:
        print(f"\n❌ [Workflow Failed] 任务执行失败，错误信息: {e}")
        # 抛出异常使得 GitHub Actions 将这次运行标记为失败，方便你接收告警
        raise e
