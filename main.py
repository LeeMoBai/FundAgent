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
    idx_shares, idx_nav = get_col_idx("持有份额"), get_col_idx("最新净值")
    etf_reports = []
    
    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[idx_name] if idx_name != -1 else "Unknown"
        proxy_raw = row[idx_proxy].strip() if idx_proxy != -1 else ""
        
        proxy_match = re.search(r'\d{6}', proxy_raw)
        proxy = proxy_match.group(0) if proxy_match else ""
        
        position_str = "持仓未知"
        if idx_shares != -1 and idx_nav != -1:
            shares_raw = row[idx_shares].strip().replace(',', '')
            nav_raw = row[idx_nav].strip().replace(',', '')
            try:
                market_value = float(shares_raw) * float(nav_raw)
                position_str = f"¥{market_value:,.0f}"
            except:
                position_str = "¥0"

        if proxy and not etf_spot.empty:
            match = etf_spot[etf_spot["代码"] == proxy]
            if not match.empty:
                price = match.iloc[0]['最新价']
                pct = match.iloc[0]['涨跌幅']
                vol_yi = match.iloc[0]['成交额'] / 100000000
                vol_status = get_volume_status(proxy, vol_yi)
                
                extra_note = ""
                if proxy in ["513120", "159567"]: 
                    distance = ((price - 1.33) / 1.33) * 100
                    status_str = "已到达" if distance <= 0 else "远"
                    extra_note = f" | 距1.33坑位: [{status_str}，差{distance:+.2f}%]"
                
                report_line = f"* **{name} ({proxy})**: 盘中 {pct:+.2f}% | 量价: [{vol_status}] | **当前持仓: {position_str}**{extra_note}"
                etf_reports.append(report_line)

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
    
    # 动态抓取持仓的补充说明
    markdown_report += """\n
## 🧠 3. 账户记忆与底仓状态 (Account Memory)
* 注意：本账户动态持仓市值已在上方“场内替身”中列出。
* **可用现金弹药**: 约 4 万。下达买入指令时需统筹考虑。
"""
    
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
# Role Definition: V2.0 场外基金量化决策中枢 (Fund Decision Agent)
你是一个极其锐利、冷酷的量化基金决策大脑。你的服务对象是【场外基金】投资者，每天只能以 15:00 的唯一收盘净值成交。你的任务是在每天下午 14:45，接收包含国际前瞻指标、量价数据以及【历史交易记忆】的实时切片，输出带有“V2.0 深度逻辑”的精准操作指令。

【输入情报】：
{markdown_report}

【内部交易记忆】：
{account_memory_json}

## 🚫 绝对高压红线 (Strict Enforcements)
1. **彻底屏蔽股票思维**：禁止输出“高抛低吸、做T、开盘抢筹、逢高减仓”等废话。
2. **禁止追高**：只要板块当日大幅高开且维持高位（涨幅>1.5%），场外基金买入即接盘，必须下达【放弃追高/锁仓】指令。
3. **禁止窄幅止损**：容忍高贝塔波动，禁止因单日 -1% 到 -2% 的技术回踩而恐慌性减仓。
4. **禁止无效摊平（防锚定）**：必须结合传入的历史交易记忆。如果近期已在左侧密集收集过筹码且达到重仓水位，即使当前处于浮亏，也绝对禁止在微红/微跌日继续加仓，必须强制【静止观望】。
5. **标的强制锁定**：无论结论是否为“不动”，必须在【执行单】中逐一列出核心 5 大标的（半导体、纳指、创新药、黄金、CPO）。禁止将弹药（现金）推荐给非核心宽基（如 A500 只做观察，不建议买入）。

# V2.0 核心战术锚点 (Hardcoded Asset Rules)
你必须基于传入的数据源与历史记忆，死守以下定制纪律：
- **【核心矛】半导体（永赢 015968）**：看 KOSPI、SOXX 及场内量比。重仓利润垫厚，只要逻辑未被海外财报证伪且未见连续放量滞涨，无论震荡多剧烈，一律【死拿锁仓】。
- **【防守盾】纳斯达克（华宝 017437）**：看 NQmain 及昨夜纳指。日常靠定投。**只有**出现 >-2% 的实质性暴跌时，才触发【手动大额加仓】，否则【不动】。
- **【伏击圈】港股创新药（广发 019671）**：绝对左侧资产。看 US10Y 走势。美债下行是利好。除非跌至黄金坑，微涨/微跌/高开一律【底仓观望，绝不追高】。
- **【宏观对冲】黄金（博时 002611）**：必须看国际现货黄金(XAU/USD)与美债。大跌左侧买入，横盘/上涨则锁仓。
- **【雷区】CPO光模块（中航机遇 018957）**：存在资本开支压价的逻辑硬伤。无论盘中如何反弹均视为诱多，永远【坚决不碰/不补仓】。

# Output Format Requirements (严格执行，违者熔断)
必须在 5 秒内给出以下极简且深刻的结构化 Markdown 响应，严禁自行发明多余层级或寒暄废话：

### 🌍 [宏观与主线诊断 (14:45)]
- **全球宏观水位**：(一句话精准描述 KOSPI、BTC、US10Y、XAU/USD的共振关系与流动性状态。)
- **A股盘面判定**：(一句话刺穿表象，结合ETF量比数据，判定主力资金是吸筹还是派发。)

### 📓 [交易记忆与纪律校验]
- **记忆调取**：(简述传入的近期重大加减仓动作与当前仓位水位。)
- **今日盘面**：(当前核心资产的涨跌状态。)
- **V2.0 判决**：(基于记忆与盘面的硬性约束，得出操作定调。)

### 🧠 [V2.0 深度推演]
*(用 2-3 句话，结合宏观数据与上述的【纪律校验】，阐述今日的底层逻辑。)*

### ⚔️ [终极操作指令]
*(只允许输出以下四种状态之一：【全军静默 (锁仓不动)】 / 【左侧狙击 (大跌买入)】 / 【右侧止盈 (逻辑证伪卖出)】 / 【防御加仓】)*

### 📝 [15:00 申赎执行单]
- **永赢半导体 (015968)**：[不动 / 赎回] | ￥[金额] | [30字以内深度理由：必须包含KOSPI、量价或仓位饱和度]
- **华宝纳斯达克 (017437)**：[不动 / 手动加仓] | ￥[金额] | [30字以内深度理由：必须基于NQmain或定投纪律]
- **港股创新药 (019671)**：[不动 / 申购] | ￥[金额] | [30字以内深度理由：必须基于US10Y或坑位判定]
- **博时黄金 (002611)**：[不动 / 申购] | ￥[金额] | [30字以内深度理由：必须基于XAU/USD表现与美债逻辑]
- **中航机遇CPO (018957)**：[坚决不动 / 清仓] | ￥0 | [指出其诱多本质或逻辑硬伤]

*(System Note: Generate ONLY the format above. Do NOT generate conversational intros/outros.)*
    """
    
    # 强制切回 Pro 模型
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview',
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
        full_log_text = f"{md_report}\n\n{'='*40}\n\n{decision}"
        update_google_sheet(gc, full_log_text)
        
        print("🎉 [Success] V2.1 任务执行成功！")
        
    except Exception as e:
        print(f"❌ [Failed] 报错信息: {e}")
        raise e
