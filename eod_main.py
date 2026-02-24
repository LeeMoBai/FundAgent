import os
import json
import datetime
import pytz
import gspread
import akshare as ak
from google import genai
import re

# ==========================================
# 0. 认证初始化
# ==========================================
def get_gspread_client():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("环境变量中缺失 GCP_SERVICE_ACCOUNT")
    return gspread.service_account_from_dict(json.loads(creds_json))

# ==========================================
# 1. 抓取真实场外净值与计算盈亏
# ==========================================
def calculate_eod_pnl(gc) -> tuple:
    sh = gc.open("基金净值总结")
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_col_idx(kw):
        return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    idx_name = get_col_idx("基金名称")
    idx_fund_code = get_col_idx("基金代码") 
    idx_shares = get_col_idx("持有份额")
    idx_nav = get_col_idx("最新净值")
    
    eod_reports = []
    total_daily_profit = 0.0
    total_market_value = 0.0
    
    print("   [+] 正在向东方财富拉取今日 22:30 场外基金真实净值结算数据...")

    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        
        name = row[idx_name] if idx_name != -1 else "Unknown"
        fund_code_raw = row[idx_fund_code].strip() if idx_fund_code != -1 else ""
        
        code_match = re.search(r'\d{6}', fund_code_raw)
        fund_code = code_match.group(0) if code_match else ""
        
        if not fund_code: continue

        shares = 0.0
        nav_yesterday = 0.0
        if idx_shares != -1 and idx_nav != -1:
            try:
                shares = float(re.sub(r'[^\d.]', '', row[idx_shares]))
                nav_yesterday = float(re.sub(r'[^\d.]', '', row[idx_nav]))
            except:
                pass
        
        real_nav = "未更新"
        daily_pct = "0.00"
        profit_str = "¥0.00"
        
        try:
            # 🎯 核心修复：正确的 API 调用和精确的缩进
            nav_df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
            if not nav_df.empty:
                last_record = nav_df.iloc[-1]
                real_nav = last_record['单位净值']
                daily_pct = last_record['日增长率']
                
                if shares > 0:
                    profit = shares * nav_yesterday * (float(daily_pct) / 100)
                    total_daily_profit += profit
                    total_market_value += (shares * float(real_nav))
                    profit_str = f"¥{profit:+.2f}"
        except Exception as e:
            print(f"抓取 {name}({fund_code}) 失败: {e}")
            
        report_line = f"* **{name} ({fund_code})**: 实际涨跌 {daily_pct}% | 真实净值 {real_nav} | **今日盈亏: {profit_str}**"
        eod_reports.append(report_line)

    markdown_report = f"## 📊 1. 场外基金 22:30 真实清算单\n"
    markdown_report += "\n".join(eod_reports)
    
    markdown_report += f"\n\n## 💰 2. 账户全局结算\n"
    markdown_report += f"* **当前总市值**: 约 ¥{total_market_value:,.0f}\n"
    markdown_report += f"* **今日总盈亏**: **¥{total_daily_profit:+.2f}**\n"
    
    return markdown_report

# ==========================================
# 2. AI 盘后归因 (The EOD Brain)
# ==========================================
def ask_eod_agent(markdown_report: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)
    
    prompt = f"""
# Role Definition: V2.0 盘后归因分析师 (EOD Quant Analyst)
当前时间是 22:30，A股场外基金已公布真实净值。请你基于下方的【今日真实清算单】，进行冷酷的盘后复盘。

【今日结算数据】：
{markdown_report}

# Output Format Requirements (严格格式)
必须给出以下极简的结构化 Markdown 响应，严禁废话：

### 🌙 [22:30 盘后清算与归因]
- **账单总结**：(一句话总结今日总盈亏金额，以及谁是今天赚钱的头号功臣/亏钱的罪魁祸首。)
- **逻辑校验**：(复盘今日 14:45 的操作是否正确。例如：庆幸白天锁仓未追高/底层逻辑依然成立。)
- **明日沙盘**：(一句话指明明日的核心监控指标。)
    """
    
    # 🎯 切换为最强推理模型 gemini-3.1-pro-preview
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview',
        contents=prompt,
        config=genai.types.GenerateContentConfig(temperature=0.2)
    )
    return f"{markdown_report}\n\n{'='*40}\n\n{response.text}"

# ==========================================
# 3. 定点回写 Google Sheet 的 C 列
# ==========================================
def write_to_column_c(gc, final_report: str):
    sh = gc.open("基金净值总结")
    ws = sh.worksheet("AI-参考")
    
    all_data = ws.get_all_values()
    if len(all_data) == 0:
        return 
        
    last_row_idx = len(all_data)
    last_row_date_str = all_data[-1][0] 
    
    tz_bj = pytz.timezone('Asia/Shanghai')
    today_str = datetime.datetime.now(tz_bj).strftime('%Y-%m-%d')
    
    if today_str in last_row_date_str:
        print(f"   [+] 找到今日 14:45 的记录，正在写入 C 列...")
        ws.update_cell(last_row_idx, 3, final_report)
    else:
        print(f"   [+] 未找到今日记录，新建一行写入...")
        ws.append_row([datetime.datetime.now(tz_bj).strftime('%Y-%m-%d %H:%M'), "白天未执行", final_report])

# ==========================================
# Workflow 主入口
# ==========================================
if __name__ == "__main__":
    print("🚀 [EOD Start] 启动 V2.0 盘后清算引擎...")
    try:
        gc = get_gspread_client()
        
        print("⏳ [1/3] 正在拉取真实净值并计算盈亏...")
        report = calculate_eod_pnl(gc)
        print(report)
        
        print("⏳ [2/3] 正在唤醒归因大脑 (gemini-3.1-pro-preview)...")
        final_log = ask_eod_agent(report)
        print("✅ 归因分析完成。")
        
        print("⏳ [3/3] 正在写入 Google Sheet (C列)...")
        write_to_column_c(gc, final_log)
        
        print("🎉 [Success] 盘后清算执行成功，晚安！")
        
    except Exception as e:
        print(f"❌ [Failed] 报错信息: {e}")
        raise e
