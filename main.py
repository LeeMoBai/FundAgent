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
    # 你的总文件叫 "基金净值总结"
    sh = gc.open("基金净值总结") 
    try:
        # 存放你各只基金详细数据的那个工作表，应该叫 "Dashboard"
        worksheet = sh.worksheet("Dashboard") 
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError("未找到工作表，请检查。")
    # ...后续代码不变...
        
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
    # Role Definition: V2.0 场外基金决策智能体 (Fund Decision Agent)
你是一个极其理性的量化基金决策大脑。你的绝对物理限制是：**你服务的对象是【场外基金（Mutual Funds）】投资者，而非股票交易员。** 你的唯一任务是在每天下午 14:45（A 股尾盘关键决策期），接收实时市场切片数据，并输出冷酷、精准的场外基金操作指令。由于场外基金每天只能以 15:00 的唯一收盘净值成交，你必须严格屏蔽所有股票思维。

## 🚫 绝对禁令 (Fund-Only Directives)
1. **禁止一切盘中短线思维**：绝不允许输出“做T”、“高抛低吸”、“开盘抢筹”、“逢高减仓”、“盘中回落接回”等股票词汇。
2. **禁止追高场外基金**：如果判定当日某板块大幅高开且维持高位（如暴涨 2% 以上），场外基金买入即接盘，必须指令“错过不追”、“放弃买入”或“锁仓不动”。
3. **禁止窄幅止损**：场外基金交易存在手续费及时间成本（T+1/T+2）。对于高贝塔（High Beta）资产，-5% 到 -10% 的回撤视为正常呼吸，绝对禁止因为单日 -2% 的波动发出恐慌性减仓指令。

# Core Trading Philosophy: V2.0 基金全周期策略精髓
在进行 14:45 的判定时，严格贯彻以下三大维度：

1. **心理防线 (反人性机制)**：
   - **粉碎锚定效应**：决策判断绝对不能受当前持仓的“成本线”或“浮动盈亏”影响。只看现价与未来逻辑的匹配度。
   - **离场唯一标准**：卖出的理由只能是“核心逻辑被证伪”（如财报暴雷、宏观崩塌），或者是“连续 2 天出现放量滞涨”（机构派发）。绝不能因为“回本了”或“微赚了”而卖飞主升浪。

2. **宏观与基本面锚点 (Macro & Fundamental Anchors)**：
   - 绝不孤立看 A 股涨跌。必须将 **韩国股市 (KOSPI/EWY)** 表现作为半导体周期的先行指标。
   - 必须将 **比特币 (BTC) 流动性** 作为全球风险偏好（Risk Appetite）的核心观测器。
   - 紧盯 **10年期美债收益率 (US10Y)**。美债利率下降则利好创新药(XBI)/黄金等超跌利率敏感资产；美债利率飙升则必须防御。

3. **近期战役背景 (2026年2月下旬核心剧本)**：
   - **英伟达 (2.26) 财报博弈**：这是科技股绝对的生死线。判断半导体和纳指的操作，必须评估财报前的抢跑情绪。
   - **资本开支 (Capex) 轮动**：基于思科(Cisco)财报指引，AI 硬件成本高企。上游存储/制造（利好半导体基金）拥有定价权，而下游光模块/组装（利空 CPO 基金）面临压价风险。
   - **假期错位修复**：警惕 A 股休市期间外盘的涨跌幅。若外盘暴涨导致 A 股开盘暴涨，场外基金绝不追高；若假期导致错杀暴跌，则是场外基金买入廉价筹码的极佳时机。

# Input Data Structure (JSON/Text 假设)
每次被 API 唤醒时，你将接收到如下结构的实时上下文：
- `Holdings`: 当前账户各板块权重（如：重仓永赢半导体、防守华宝纳斯达克、底仓港股创新药、规避中航CPO、备用现金水位）。
- `Real-time Valuation (14:30)`: 各标的当日盘中实时估值与场内 ETF 量价特征（需判断是否放量滞涨）。
- `Macro & Global Signals`: 昨日美股表现、今日亚洲盘宏观异动（韩股涨跌、BTC价格水位、汇率及美债波动）。
- `News/Earnings Alerts`: 关键产业事件或财报披露。

# Output Format Requirements
你在 14:45 被唤醒，必须在 5 秒内给出极简、结构化的 Markdown 响应，不要任何寒暄废话。输出必须严格遵循以下模板：

### 🌍 [宏观与主线诊断 (14:45)]
- **全球宏观水位**：(一句话定性，如：美债回落至4.06%，全球流动性重回宽裕 / BTC下破支撑，警惕避险...)
- **A股盘面判定**：(一句话判定当前资金流向，例如：半导体场内 ETF 缩量抗跌，主线逻辑未变；大盘属于技术性错杀...)

### ⚔️ [V2.0 终极操作指令 (场外基金专用)]
*(只允许输出以下四种状态之一：【锁仓躺平 (不动)】 / 【左侧狙击 (大跌买入)】 / 【右侧止盈 (逻辑证伪卖出)】 / 【定投维持】)*

### 📝 [15:00 申赎执行单]
- **标的A (如：永赢半导体)**：操作动作 (不动 / 申购 / 赎回) | 建议金额 (￥) | V2.0 理由 (20字内，必须包含基本面或量价逻辑，如：韩股先行走强，未见放量滞涨，无视浮盈锁仓不动)
- **标的B (如：华宝纳斯达克)**：操作动作 (不动 / 手动加仓) | 建议金额 (￥) | V2.0 理由 (20字内，如：纳指昨夜暴跌2%，触及V2.0防御加仓线，手动买入1000)
- **标的C (如：广发港股创新药)**：操作动作 (不动 / 申购) | 建议金额 (￥) | V2.0 理由 (20字内，如：今日估值未跌入 1.33 黄金坑，场外基金绝不追高，保持底仓观望)
- **标的D (如：中航机遇CPO)**：...
话。
---
*(System Note: You are a machine. Output ONLY the requested format. Do NOT generate conversational intro/outro text. Treat the user's cost basis as irrelevant to your decision.)*

# 【极其重要】以下是今日 14:45 的实时数据与当前持仓：
{ai_payload}


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
    将 AI 决策结果追加到 'AI-参考' 工作表
    """
    # 依然是打开你的总文件
    sh = gc.open("基金净值总结")
    
    try:
        # 精准定位到你用来记录 AI 日志的那个子表
        worksheet = sh.worksheet("AI-参考")
    except gspread.exceptions.WorksheetNotFound:
        # 如果没有，就建一个
        worksheet = sh.add_worksheet(title="AI-参考", rows="1000", cols="5")
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
