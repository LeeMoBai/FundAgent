import os
import json
import datetime
import pytz
import gspread
import akshare as ak
from google import genai
import re
import requests

# ==========================================
# 0. 认证初始化
# ==========================================
def get_gspread_client():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("缺失 GCP_SERVICE_ACCOUNT")
    return gspread.service_account_from_dict(json.loads(creds_json))

# ==========================================
# 1. 抓取真实净值、盈亏与今日交易记录
# ==========================================
def collect_eod_v3_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    dash_data = sh.worksheet("Dashboard").get_all_values()
    headers = dash_data[0]
    
    def get_col_idx(kw): 
        return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    idx_name = get_col_idx("基金名称")
    idx_shares = get_col_idx("持有份额")
    idx_nav = get_col_idx("最新净值")
    idx_fund_code = get_col_idx("基金代码") if get_col_idx("基金代码") != -1 else get_col_idx("替身代码")
    
    eod_reports = []
    asset_breakdown = []
    total_daily_profit = 0.0
    total_market_value = 0.0
    
    print("   [+] 拉取东方财富 22:30 场外真实结算数据...")

    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): 
            continue
            
        name = row[idx_name] if idx_name != -1 else "Unknown"
        fund_code_raw = row[idx_fund_code].strip() if idx_fund_code != -1 else ""
        fund_code = re.search(r'\d{6}', fund_code_raw).group(0) if re.search(r'\d{6}', fund_code_raw) else ""
        
        if not fund_code: 
            continue

        shares = 0.0
        nav_yesterday = 0.0
        if idx_shares != -1 and idx_nav != -1 and len(row) > idx_shares and len(row) > idx_nav:
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
            print(f"抓取 {name} 失败: {e}")
            
        # 🔴 战术级：Markdown 简报
        eod_reports.append(f"* **{name} ({fund_code})**: 涨跌 {daily_pct}% | 盈亏 {profit_str}")
        
        # 🔵 战略级：JSON 明细
        asset_breakdown.append({
            "name": name,
            "fund_code": fund_code,
            "daily_pct": float(daily_pct) if daily_pct != "0.00" else 0.0,
            "realized_pnl": profit_str,
            "nav_settled": real_nav
        })

    # 读取今日交易动作
    todays_trades = []
    try:
        trade_data = sh.worksheet("交易记录").get_all_values()
        now = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
        formats = [now.strftime('%Y-%m-%d'), f"{now.year}/{now.month}/{now.day}", now.strftime('%Y/%m/%d')]
        
        for row in trade_data[1:]:
            if row and row[0].strip() and any(f in row[0] for f in formats):
                todays_trades.append(" | ".join(row[1:]))
    except Exception as e: 
        print(f"交易记录读取异常: {e}")

    # 组装极简 Prompt
    trades_str = "\n".join([f"- {t}" for t in todays_trades]) if todays_trades else "今日无任何买卖操作记录，全天静默。"
    reports_str = "\n".join(eod_reports)
    
    md_prompt = f"""## 📊 1. 22:30 账户清算快照
* **全局总市值**: 约 ¥{total_market_value:,.0f}
* **今日总盈亏**: **¥{total_daily_profit:+.2f}**
{reports_str}

## 📝 2. 今日真实交易动作
{trades_str}
"""
    
    # 战略级 JSON 底稿
    strategic_json = {
        "account_summary": {
            "total_market_value": round(total_market_value, 2),
            "total_daily_pnl": round(total_daily_profit, 2)
        },
        "asset_breakdown": asset_breakdown,
        "actual_trades": todays_trades
    }
    
    return md_prompt, strategic_json

# ==========================================
# 2. AI 极简盘后审计大脑
# ==========================================
def ask_v3_eod_agent(md_prompt: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V3.0 盘后归因与风控审计师 (EOD Quant Auditor)
当前时间是 22:30，请基于下方极简数据，输出冷酷的复盘与明日沙盘。严禁短线股票思维与废话。

【输入情报】：
{md_prompt}

# Output Format:
### 🌙 [今日动作审计]
(评价主人的操作是否遵守了纪律，规避了多少风险，或指责沉没成本谬误)
### 🔍 [板块归因探因]
(一句话：穿透表面涨跌，分析深层驱动力，如流动性或外盘映射)
### 🛡️ [明日风控沙盘]
(设定明确的【逻辑证伪条件】，如：若明日XX跌破XX，则右侧退潮确立)
    """
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(temperature=0.1)
    )
    return response.text

# ==========================================
# 3. 落盘归档与企微推送 (V3.0)
# ==========================================
def archive_and_notify_eod(md_prompt: str, ai_decision: str, strategic_json: dict):
    tz_bj = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_bj)
    time_prefix = now.strftime('%Y-%m-%d_%H%M')
    
    # 将 AI 的复盘也存入 JSON 底稿
    strategic_json["ai_audit_and_sandbox"] = ai_decision
    
    os.makedirs("logs", exist_ok=True)
    json_path = f"logs/{time_prefix}_EOD_Strategic.json"
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(strategic_json, f, ensure_ascii=False, indent=2)
    print(f"📦 EOD 战略级 JSON 已入库: {json_path}")

    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if robot_key:
        payload = {
            "msgtype": "markdown", 
            "markdown": {
                "content": f"<font color='info'>**🌙 V3.0 晚间审计战报 (影分身测试)**</font>\n\n{md_prompt}\n{'='*20}\n{ai_decision}"
            }
        }
        try:
            url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}"
            requests.post(url, json=payload)
            print("📡 企微推送成功！")
        except Exception as e:
            print(f"❌ 企微推送网络异常: {e}")

# ==========================================
# 主流程
# ==========================================
if __name__ == "__main__":
    print("🚀 [V3.0 Shadow Run] 启动 EOD 盘后双轨测试引擎...")
    try:
        gc = get_gspread_client()
        
        md_prompt, strategic_json = collect_eod_v3_intelligence(gc)
        
        print("⏳ 唤醒审计大脑计算复盘...")
        ai_decision = ask_v3_eod_agent(md_prompt)
        
        print("⏳ 执行 JSON 落盘与微信分发...")
        archive_and_notify_eod(md_prompt, ai_decision, strategic_json)
        
        print("🎉 V3.0 EOD 影子测试运行完毕！")
    except Exception as e:
        print(f"❌ 运行失败: {e}")
        raise e
