import os
import json
import datetime
import pytz
import gspread
import akshare as ak
from google import genai
import re
import requests  # 🎯 新增：用于发送企业微信请求

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
def calculate_eod_pnl(gc) -> str:
    sh = gc.open("基金净值总结")
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_col_idx(kw):
        return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    idx_name = get_col_idx("基金名称")
    idx_fund_code = get_col_idx("基金代码") if get_col_idx("基金代码") != -1 else get_col_idx("替身代码")
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
# 1.5 读取今日实际执行的交易动作 (极简兼容版)
# ==========================================
def get_todays_trades(gc) -> str:
    try:
        sh = gc.open("基金净值总结")
        trade_data = sh.worksheet("交易记录").get_all_values()
        
        tz_bj = pytz.timezone('Asia/Shanghai')
        now = datetime.datetime.now(tz_bj)
        
        # 精准匹配你的 2026/02/24 习惯，不搞暴力替换
        format_1 = now.strftime('%Y-%m-%d')
        format_2 = f"{now.year}/{now.month}/{now.day}"
        format_3 = now.strftime('%Y/%m/%d')
        
        todays_trades = []
        for row in trade_data[1:]:
            if not row or not row[0].strip(): continue
            if format_1 in row[0] or format_2 in row[0] or format_3 in row[0]:
                todays_trades.append(row)
        
        if todays_trades:
            trades_str = "\n".join([f"- 动作: " + " | ".join(row[1:]) for row in todays_trades])
            return trades_str
        else:
            return "今日无任何买卖操作记录。"
    except Exception as e:
        return f"交易记录读取异常: {e}"

# ==========================================
# 2. AI 盘后归因 (V2.3 极致冷酷审计版 - 剔除点位幻觉)
# ==========================================
def ask_eod_agent(markdown_report: str, todays_trades: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)
    
    prompt = f"""
# Role Definition: V2.3 晚间归因与风控审计师 (EOD Quant Auditor)
当前时间是 22:30，你负责对今日真实净值进行冷酷复盘。

## ⚠️ 绝对高压红线 (Hard Bans)
1. **严禁预测点位**：禁止输出任何形如“跌破 X.XXX 点位”或“下探至 X%”的预测，那是场外基金的伪命题。
2. **严禁止损建议**：禁止提出“无条件减仓、止损、高抛低吸”等短线股票思维。
3. **严禁情绪干扰**：复盘必须基于“概率分层”和“逻辑证伪”，剔除所有马屁和废话。

# 📋 输入情报
【今日真实清算单】：
{markdown_report}

【今日实际交易动作】：
{todays_trades}

# Output Format Requirements (严格执行)
请根据以下结构，给出极其精炼的复盘：

### 🌙 [22:30 晚间审计与策略校准]
- **今日动作审计**：(基于【今日实际交易动作】进行冷酷审计。分析该动作是否符合长期逻辑，规避了多少回撤风险。使用概率置信度。)
- **板块归因探因**：(穿透表面涨跌，分析全球宏观、科技巨头资本开支、韩国KOSPI映射等深层驱动力。)
- **明日风控沙盘**：(严禁点位！必须基于【宏观事件】和【逻辑证伪】。结论只能是：继续锁仓 / 准备左侧接回 / 逻辑证伪观察。设定明确的【逻辑证伪条件】。)
    """
    
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview',
        contents=prompt,
        config=genai.types.GenerateContentConfig(temperature=0.1)
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
# 4. 🎯 新增：企业微信 Markdown 推送
# ==========================================
def send_wechat_robot(content: str):
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if not robot_key:
        print("⚠️ 未配置 WECHAT_ROBOT_KEY，跳过企业微信推送。")
        return
        
    url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}"
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": f"<font color=\"info\">**V2.3 晚间审计战报已送达**</font>\n\n{content}"
        }
    }
    try:
        res = requests.post(url, json=payload)
        if res.json().get("errcode") == 0:
            print("📡 企业微信推送成功！")
        else:
            print(f"❌ 企业微信推送失败: {res.json()}")
    except Exception as e:
        print(f"❌ 微信推送网络异常: {e}")

# ==========================================
# Workflow 主入口
# ==========================================
if __name__ == "__main__":
    print("🚀 [EOD Start] 启动 V2.3 盘后清算与审计引擎...")
    try:
        gc = get_gspread_client()
        
        print("⏳ [1/5] 正在拉取真实净值并计算盈亏...")
        report = calculate_eod_pnl(gc)
        print(report)
        
        print("⏳ [2/5] 正在读取今日实际交易动作...")
        todays_trades = get_todays_trades(gc)
        print(f"   [今日动作]: \n{todays_trades}")
        
        print("⏳ [3/5] 正在唤醒审计大脑 (gemini-3.1-pro-preview)...")
        final_log = ask_eod_agent(report, todays_trades)
        print("✅ 归因分析完成。")
        
        print("⏳ [4/5] 正在写入 Google Sheet (C列)...")
        write_to_column_c(gc, final_log)
        
        print("⏳ [5/5] 正在向企业微信发送战报...")
        send_wechat_robot(final_log)
        
        print("🎉 [Success] 盘后清算任务执行成功！")
        
    except Exception as e:
        print(f"❌ [Failed] 报错信息: {e}")
        raise e
