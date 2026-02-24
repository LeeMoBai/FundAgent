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
        "US10Y": "^TNX",           
        "BTC": "BTC-USD",          
        "KOSPI": "^KS11",          
        "XAU_USD": "GC=F",         
        "NQmain": "NQ=F",          
        "XBI": "XBI"               
    }

    for key, symbol in tickers.items():
        try:
            # 🎯 拉取过去 5 天，防止周末/节假日抓不到最新K线
            data = yf.Ticker(symbol).history(period="5d")
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
    try:
        hist_df = ak.fund_etf_hist_em(symbol=proxy_code, period="daily")
        if len(hist_df) >= 2:
            yesterday_vol_yi = hist_df.iloc[-2]['成交额'] / 100000000
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
# 3. 核心：计算年线乖离率与1年水位分位 (雷达专属)
# ==========================================
def get_radar_technical_data(proxy_code: str, current_price: float) -> str:
    try:
        hist_df = ak.fund_etf_hist_em(symbol=proxy_code, period="daily")
        if len(hist_df) >= 250:
            recent_250 = hist_df.tail(250)
        elif len(hist_df) >= 20:
            recent_250 = hist_df 
        else:
            return "上市过短无数据"
            
        ma250 = recent_250['收盘'].mean()
        ma250_dist = ((current_price - ma250) / ma250) * 100
        
        high_250 = recent_250['最高'].max()
        low_250 = recent_250['最低'].min()
        
        if high_250 != low_250:
            price_percentile = ((current_price - low_250) / (high_250 - low_250)) * 100
        else:
            price_percentile = 50.0
            
        return f"距年线: {ma250_dist:+.2f}% | 1年绝对水位: {price_percentile:.1f}%"
    except Exception as e:
        return f"探测失败"

# ==========================================
# 4. 抓取持仓与雷达并组装动态简报
# ==========================================
def collect_full_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    macro = get_macro_data()
    
    print("   [+] 正在拉取全市场 ETF 实时快照...")
    try:
        etf_spot = ak.fund_etf_spot_em()
    except:
        etf_spot = pd.DataFrame()

    # --- 处理现役底仓 (Dashboard) 并动态组装规则 ---
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers_dash = dash_data[0]
    
    def get_col_idx(headers, kw):
        return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    idx_name = get_col_idx(headers_dash, "基金名称")
    idx_proxy = get_col_idx(headers_dash, "替身代码")
    idx_shares = get_col_idx(headers_dash, "持有份额")
    idx_nav = get_col_idx(headers_dash, "最新净值")
    idx_rule = get_col_idx(headers_dash, "战术纪律") # 🎯 动态战术列
    
    etf_reports = []
    dynamic_rules = []
    dynamic_exec_template = []
    
    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[idx_name] if idx_name != -1 else "Unknown"
        proxy_raw = row[idx_proxy].strip() if idx_proxy != -1 else ""
        proxy = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        
        # 🎯 动态组装战术纪律与输出模板
        rule_text = row[idx_rule].strip() if idx_rule != -1 and len(row) > idx_rule else "结合宏观与量价数据严格执行纪律"
        if rule_text:
            dynamic_rules.append(f"- **【{name}】**：{rule_text}")
        dynamic_exec_template.append(f"- **{name}**：[不动 / 申赎买卖] | ￥[金额] | [30字深度理由：必须紧扣专属纪律]")
        
        position_str = "持仓未知"
        if idx_shares != -1 and idx_nav != -1:
            shares_raw = re.sub(r'[^\d.]', '', row[idx_shares]) if len(row) > idx_shares else ""
            nav_raw = re.sub(r'[^\d.]', '', row[idx_nav]) if len(row) > idx_nav else ""
            try:
                if shares_raw and nav_raw:
                    position_str = f"¥{(float(shares_raw) * float(nav_raw)):,.0f}"
                else:
                    position_str = "¥0(净值延迟)"
            except:
                position_str = "¥0(异常)"

        if proxy and not etf_spot.empty:
            match = etf_spot[etf_spot["代码"] == proxy]
            if not match.empty:
                price = match.iloc[0]['最新价']
                pct = match.iloc[0]['涨跌幅']
                vol_status = get_volume_status(proxy, match.iloc[0]['成交额'] / 100000000)
                etf_reports.append(f"* **{name} ({proxy})**: 盘中 {pct:+.2f}% | 量价: [{vol_status}] | **当前持仓: {position_str}**")

    # --- 处理雷达监控池 (雷达监控) ---
    print("   [+] 正在启动 V2.0 深潜雷达探测器...")
    radar_reports = []
    try:
        ws_radar = sh.worksheet("雷达监控")
        radar_data = ws_radar.get_all_values()
        headers_radar = radar_data[0]
        
        r_idx_name = get_col_idx(headers_radar, "板块名称")
        r_idx_proxy = get_col_idx(headers_radar, "替身代码")
        r_idx_logic = get_col_idx(headers_radar, "核心伏击逻辑")
        r_idx_trigger = get_col_idx(headers_radar, "狙击触发条件")
        
        for row in radar_data[1:]:
            if not row or not any(row): continue
            r_name = row[r_idx_name] if r_idx_name != -1 else "Unknown"
            r_proxy_raw = row[r_idx_proxy] if r_idx_proxy != -1 else ""
            r_logic = row[r_idx_logic] if r_idx_logic != -1 else "无"
            r_trigger = row[r_idx_trigger] if r_idx_trigger != -1 else "无"
            
            r_proxy = re.search(r'\d{6}', r_proxy_raw).group(0) if re.search(r'\d{6}', r_proxy_raw) else ""
            
            if r_proxy and not etf_spot.empty:
                match = etf_spot[etf_spot["代码"] == r_proxy]
                if not match.empty:
                    r_price = match.iloc[0]['最新价']
                    r_pct = match.iloc[0]['涨跌幅']
                    tech_str = get_radar_technical_data(r_proxy, float(r_price))
                    radar_reports.append(f"* **{r_name} ({r_proxy})**: 盘中 {r_pct:+.2f}% | 【估值探测】: {tech_str} \n  * 逻辑: {r_logic} \n  * 🎯 **扳机**: {r_trigger}")
    except Exception as e:
        radar_reports.append(f"雷达表读取异常: {e}")

    # --- 提取内部记忆 (前10笔) ---
    try:
        trade_data = sh.worksheet("交易记录").get_all_values()
        valid_trades = [r for r in trade_data[1:] if any(r) and r[0].strip() != ""]
        recent_10_trades = valid_trades[:10]
    except:
        recent_10_trades = ["无数据"]

    # --- 拼装 Markdown ---
    markdown_report = f"""## 🌍 1. 全球宏观水位 (Macro)
* **10年期美债 (US10Y)**: {macro.get('US10Y', 'N/A')}
* **比特币 (BTC)**: {macro.get('BTC', 'N/A')}
* **韩国KOSPI (半导体先行)**: {macro.get('KOSPI', 'N/A')}
* **国际金价 (XAU/USD)**: {macro.get('XAU_USD', 'N/A')}
* **纳指100期货 (NQmain)**: {macro.get('NQmain', 'N/A')} 
* **美股生物科技 (XBI)**: {macro.get('XBI', 'N/A')}

## 🎯 2. 核心场内替身盘中表现 (ETF Proxies)
"""
    markdown_report += "\n".join(etf_reports)
    
    markdown_report += "\n\n## 📡 3. V2.0 雷达监控池 (4万备用金狩猎区)\n"
    if radar_reports:
        markdown_report += "\n".join(radar_reports)
    else:
        markdown_report += "雷达池未配置或为空。"
    
    markdown_report += """\n
## 🧠 4. 账户记忆与底仓状态 (Account Memory)
* 注意：本账户动态持仓市值已在上方“场内替身”中列出。
* **可用现金弹药**: 约 4 万。下达雷达狙击指令时需统筹考虑。
"""
    
    rules_str = "\n".join(dynamic_rules)
    exec_str = "\n".join(dynamic_exec_template)
    account_memory_json = json.dumps({"最近10笔真实交易记录": recent_10_trades}, ensure_ascii=False)
    
    return markdown_report, account_memory_json, rules_str, exec_str

# ==========================================
# 5. AI 决策中枢 (动态指令注入)
# ==========================================
def ask_fund_agent(markdown_report: str, account_memory_json: str, rules_str: str, exec_str: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)
    
    prompt = f"""
# Role Definition: V2.0 场外基金量化决策中枢 (Fund Decision Agent)
你是一个极其锐利、冷酷的量化基金决策大脑。你在下午 14:45 接收数据，输出带有“V2.0 深度逻辑”的精准操作指令。

【输入情报】：
{markdown_report}
【内部交易记忆】：
{account_memory_json}

## 🚫 绝对高压红线 (Strict Enforcements)
1. **禁止追高**：只要板块大幅高开且维持高位（涨幅>1.5%），必须下达【放弃追高/锁仓】。
2. **禁止无效摊平**：如果近期已左侧密集收集且达到重仓，处于浮亏也绝对禁止微红/微跌加仓。
3. **标的强制锁定**：无论结论是否为“不动”，必须在【执行单】中逐一列出下方模板中的所有现役标的。禁止遗漏，禁止推荐非持仓宽基。

# 🛡️ 现役阵地专属战术纪律 (Dynamic Asset Rules)
你必须基于传入的数据源与历史记忆，死守以下定制纪律：
{rules_str}

# Output Format Requirements (严格执行，违者熔断)
必须给出以下极简且深刻的结构化 Markdown，严禁废话：

### 🌍 [宏观与主线诊断 (14:45)]
- **全球宏观水位**：(一句话精准描述流动性状态)
- **A股盘面判定**：(结合ETF量比数据，判定主力意图)

### 🧠 [现役阵地深度推演与执行]
*(用 2 句话，结合宏观与记忆，阐述现有持仓今日的底层逻辑。随后直接输出执行单)*
### 📝 [15:00 申赎执行单]
{exec_str}

### 🎯 [雷达池狙击信号 (4万备用金专属)]
*(仔细对比雷达池的【估值探测数据】与【🎯 扳机】。若无完全吻合标的，回复：“全网雷达未触发定量扳机，4万备用金继续锁定静默”。若有吻合，给出建仓指令。)*
    """
    
    # 🎯 切换为最强推理模型 gemini-3.1-pro-preview
    # 配合 temperature=0.1，输出极其精准、不发散
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview',
        contents=prompt,
        config=genai.types.GenerateContentConfig(temperature=0.1)
    )
    return response.text

# ==========================================
# 6. 回写 Google Sheet
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
    print("🚀 [Workflow Start] 启动 V2.0 深潜雷达全息量化引擎...")
    try:
        gc = get_gspread_client()
        
        md_report, memory_json, rules_str, exec_str = collect_full_intelligence(gc)
        print("\n" + "="*50)
        print("👇 14:45 真实盘面、量价与雷达探测情报 👇")
        print(md_report)
        print("="*50 + "\n")
        
        print(f"⏳ 正在唤醒受高压线约束的 AI Agent (gemini-3.1-pro-preview)...")
        decision = ask_fund_agent(md_report, memory_json, rules_str, exec_str)
        print("✅ AI 决策完成。")
        
        full_log_text = f"{md_report}\n\n{'='*40}\n\n{decision}"
        update_google_sheet(gc, full_log_text)
        
        print("🎉 [Success] V2.0 雷达任务执行成功！")
        
    except Exception as e:
        print(f"❌ [Failed] 报错信息: {e}")
        raise e
