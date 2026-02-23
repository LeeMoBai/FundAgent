import os
import json
import datetime
import pytz
import gspread
import akshare as ak
import yfinance as yf
from google import genai
import pandas as pd
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
# 1. 抓取全球宏观数据 (包含真实底层资产)
# ==========================================
def get_macro_data() -> dict:
    macro = {}
    tickers = {
        "US10Y": "^TNX",           # 10年期美债
        "BTC": "BTC-USD",          # 比特币
        "KOSPI": "^KS11",          # 韩国KOSPI (半导体先行)
        "XAU_USD": "GC=F",         # COMEX黄金期货 (国际金价)
        "NQmain": "NQ=F",          # 纳指100主力期货
        "XBI": "XBI"               # 美股生物科技 (创新药风向标)
    }
    
    for key, symbol in tickers.items():
        try:
            data = yf.Ticker(symbol).history(period="2d")
            if len(data) >= 2:
                prev_close = data['Close'].iloc[-2]
                current = data['Close'].iloc[-1]
                pct_chg = ((current - prev_close) / prev_close) * 100
                
                if key == "US10Y":
                    macro[key] = f"{current:.3f}% ({pct_chg:+.2f}%)"
                elif key == "BTC":
                    macro[key] = f"${current:,.0f} ({pct_chg:+.2f}%)"
                elif key == "XAU_USD":
                    macro[key] = f"${current:,.2f}/盎司 ({pct_chg:+.2f}%)"
                else:
                    macro[key] = f"{current:.2f} ({pct_chg:+.2f}%)"
            else:
                macro[key] = "N/A"
        except:
            macro[key] = "拉取失败"
            
    return macro

# ==========================================
# 2. 动态计算量比 (Volume Ratio)
# ==========================================
def get_volume_status(proxy_code: str, current_vol_yi: float) -> str:
    """
    对比昨日全天成交额，计算量比状态
    """
    try:
        # 获取该 ETF 历史日线数据
        hist_df = ak.fund_etf_hist_em(symbol=proxy_code, period="daily")
        if len(hist_df) >= 2:
            yesterday_vol_yi = hist_df.iloc[-2]['成交额'] / 100000000 # 换算为亿
            if yesterday_vol_yi > 0:
                ratio = current_vol_yi / yesterday_vol_yi
                if ratio > 1.2:
                    return f"异常放量 (量比 {ratio:.2f})"
                elif ratio < 0.8:
                    return f"缩量回踩 (量比 {ratio:.2f})"
                else:
                    return f"温和平量 (量比 {ratio:.2f})"
    except:
        pass
    return "量比暂无"

# ==========================================
# 3. 抓取持仓并组装侦察简报
# ==========================================
def collect_full_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tz_bj = pytz.timezone('Asia/Shanghai')
    today_time = datetime.datetime.now(tz_bj).strftime('%Y-%m-%d %H:%M')
    
    # 抓取宏观
    macro = get_macro_data()
    
    print("   [+] 正在拉取全市场 ETF 实时快照及量比计算...")
    try:
        etf_spot = ak.fund_etf_spot_em()
    except:
        etf_spot = pd.DataFrame()

    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_col_idx(kw):
        return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    idx_name, idx_proxy = get_col_idx("基金名称"), get_col_idx("替身代码")
    
    etf_reports = []
    
    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[idx_name] if idx_name != -1 else "Unknown"
        proxy_raw = row[idx_proxy].strip() if idx_proxy != -1 else ""
        
        # 正则提取6位纯数字代码
        proxy_match = re.search(r'\d{6}', proxy_raw)
        proxy = proxy_match.group(0) if proxy_match else ""
        
        if proxy and not etf_spot.empty:
            match = etf_spot[etf_spot["代码"] == proxy]
            if not match.empty:
                price = match.iloc[0]['最新价']
                pct = match.iloc[0]['涨跌幅']
                vol_yi = match.iloc[0]['成交额'] / 100000000
                
                # 计算真实量比状态
                vol_status = get_volume_status(proxy, vol_yi)
                
                # 特定坑位逻辑
                extra_note = ""
                if proxy in ["513120", "159567"]: 
                    distance = ((price - 1.33) / 1.33) * 100
                    status_str = "已到达" if distance <= 0 else "远"
                    extra_note = f" | 距1.33坑位: [{status_str}，差{distance:+.2f}%]"
                
                report_line = f"* **{name}替身 ({proxy})**: 现价 {price} | 涨跌幅 {pct:+.2f}% | 量价: [{vol_status}]{extra_note}"
                etf_reports.append(report_line)

    # 提取内部记忆
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

    # 拼装极其严格的 Markdown
    markdown_report = f"""## 🌍 1. 全球宏观水位 (Macro)
* **10年期美债 (US10Y)**: {macro.get('US10Y', 'N/A')}
* **比特币 (BTC)**: {macro.get('BTC', 'N/A')}
* **韩国KOSPI (半导体先行)**: {macro.get('KOSPI', 'N/A')}
* **国际金价 (XAU/USD)**: {macro.get('XAU_USD', 'N/A')} (替代场内黄金ETF)
* **纳指100期货 (NQmain)**: {macro.get('NQmain', 'N/A')} 
* **美股生物科技 (XBI)**: {macro.get('XBI', 'N/A')} (创新药真实风向标)

## 🎯 2. 核心场内替身盘中表现 (ETF Proxies)
"""
    markdown_report += "\n".join(etf_reports)
    
    account_memory_json = json.dumps({
        "近期3天账户走势": recent_3_days,
        "最近5笔真实交易记录": recent_5_trades
    }, ensure_ascii=False)

    return markdown_report, account_memory_json

# ==========================================
# 4. AI 决策中枢 (加上高压紧箍咒)
# ==========================================
def ask_fund_agent(markdown_report: str, account_memory_json: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)
    
    prompt = f"""
# Role Definition: V2.0 场外基金决策智能体
你是一个冷酷无情的量化执行机器。你的任务是根据外部宏观数据和内部交易记录，输出精准的交易指令。

【输入情报】：
{markdown_report}

【内部交易记忆】：
{account_memory_json}

🛑 🚨 【格式与纪律高压线 (Strict Enforcements) - 违者直接销毁】🚨 🛑
1. 必须且只能 输出账户现有的 5 大核心标的（永赢半导体、华宝纳斯达克、港股创新药、中航CPO、博时黄金）的执行单。
2. 绝对禁止遗漏任何一个核心标的。
3. 绝对禁止推荐、分析或买入其他边缘宽基（如 A500、红利低波）。
4. 执行单的格式必须严格遵守下方模板，禁止发明“脑状态”、“检查单”、“极度理性模式已激活”等任何废话层级。

### 🌍 [宏观与主线诊断 (14:45)]
(用两句话总结核心宏观异动对 5 大标的的影响，禁止废话)

### 📝 [15:00 申赎执行单]
- **永赢半导体**：[买入X元 / 坚决不动] | V2.0理由：(限制30字以内，直接说核心数据逻辑)
- **华宝纳斯达克**：[买入X元 / 坚决不动] | V2.0理由：(限制30字以内，直接说核心数据逻辑)
- **港股创新药**：[买入X元 / 坚决不动] | V2.0理由：(限制30字以内，直接说核心数据逻辑)
- **中航CPO(机遇领航)**：[买入X元 / 坚决不动] | V2.0理由：(限制30字以内，直接说核心数据逻辑)
- **博时黄金**：[买入X元 / 坚决不动] | V2.0理由：(限制30字以内，直接说核心数据逻辑)
    """
    
    response = client.models.generate_content(
        model='ggemini-3.1-pro-preview',
        contents=prompt
    )
    return response.text

# ==========================================
# 5. 回写 Google Sheet
# ==========================================
def update_google_sheet(gc, full_log_text: str):
    sh = gc.open("基金净值总结")
    try:
        ws = sh.worksheet("AI-参考")
    except:
        ws = sh.add_worksheet("AI-参考", 1000, 5)
    
    today_str = datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M')
    ws.append_row([today_str, full_log_text])

# ==========================================
# Workflow 主入口
# ==========================================
if __name__ == "__main__":
    print("🚀 [Workflow Start] 启动 V2.1 究极量化引擎...")
    try:
        gc = get_gspread_client()
        
        print("⏳ [1/3] 正在生成带有量比与真实锚点的侦察情报...")
        md_report, memory_json = collect_full_intelligence(gc)
        
        print("\n" + "="*50)
        print("👇 14:45 真实盘面与量价情报 👇")
        print(md_report)
        print("="*50 + "\n")
        
        print("⏳ [2/3] 正在唤醒受高压线约束的 AI Agent...")
        decision = ask_fund_agent(md_report, memory_json)
        print("✅ AI 决策完成。")
        
        print("⏳ [3/3] 正在将情报与决策缝合写入 Google Sheets...")
        # 缝合原始数据与 AI 的分析结论
        full_log_text = f"{md_report}\n\n{'='*40}\n\n{decision}"
        update_google_sheet(gc, full_log_text)
        
        print("🎉 [Success] V2.1 任务执行成功！")
        
    except Exception as e:
        print(f"❌ [Failed] 报错信息: {e}")
        raise e
