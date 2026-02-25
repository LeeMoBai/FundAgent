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
# 1. 抓取宏观数据 (定制化命名)
# ==========================================
def get_macro_v3() -> tuple:
    tactical_macro = {}
    strategic_macro = {}
    # 按照你的 V2.2 经典命名对照表
    tickers_map = {
        "10年期美债 (US10Y)": "^TNX", 
        "比特币 (BTC)": "BTC-USD", 
        "韩国KOSPI (半导体先行)": "^KS11", 
        "国际金价 (XAU/USD)": "GC=F", 
        "纳指100期货 (NQmain)": "NQ=F", 
        "美股生物科技 (XBI)": "XBI"
    }
    
    for display_name, symbol in tickers_map.items():
        try:
            data = yf.Ticker(symbol).history(period="5d")
            if len(data) >= 2:
                closes = data['Close'].tolist()
                pct_chg = ((closes[-1] - closes[-2]) / closes[-2]) * 100
                
                if "BTC" in display_name or "XAU" in display_name:
                    tactical_macro[display_name] = f"${closes[-1]:,.2f} ({pct_chg:+.2f}%)"
                elif "US10Y" in display_name:
                    tactical_macro[display_name] = f"{closes[-1]:.3f}% ({pct_chg:+.2f}%)"
                else:
                    tactical_macro[display_name] = f"{closes[-1]:.2f} ({pct_chg:+.2f}%)"
                
                # JSON用短Key
                short_key = symbol
                strategic_macro[short_key] = {"current": closes[-1], "daily_pct": pct_chg, "5d_trend": [round(x, 3) for x in closes]}
            else:
                tactical_macro[display_name] = "N/A"
        except:
            tactical_macro[display_name] = "数据超时"
            
    return tactical_macro, strategic_macro

# ==========================================
# 2. 抓取 ETF 5日量价 (YFinance 双擎容灾)
# ==========================================
def get_etf_5d_features(proxy_code: str) -> dict:
    features = {"today_pct": 0.0, "is_abnormal_vol": False, "tactical_desc": "温和平量", "5d_prices": [], "5d_vol_ratios": [], "ma250_dist": 0.0}
    try:
        hist_df = ak.fund_etf_hist_em(symbol=proxy_code, period="daily")
        if len(hist_df) < 2: raise ValueError()
        vols = hist_df.tail(6)['成交额'].tolist()
        prices = hist_df.tail(6)['收盘'].tolist()
    except:
        try:
            suffix = ".SS" if proxy_code.startswith("5") else ".SZ"
            df_yf = yf.Ticker(f"{proxy_code}{suffix}").history(period="6d")
            if len(df_yf) < 2: raise ValueError()
            vols = df_yf['Volume'].tolist()
            prices = df_yf['Close'].tolist()
        except:
            features["tactical_desc"] = "数据盲区"
            return features

    pct = ((prices[-1] - prices[-2]) / prices[-2]) * 100
    features["today_pct"] = round(pct, 2)
    
    ratios = [round(vols[i]/vols[i-1], 2) if vols[i-1]>0 else 1.0 for i in range(1, len(vols))]
    features["5d_prices"] = [round(p, 3) for p in prices[1:]]
    features["5d_vol_ratios"] = ratios
    
    if ratios[-1] > 1.2: features["tactical_desc"] = f"放量 (量比 {ratios[-1]})"
    elif ratios[-1] < 0.8: features["tactical_desc"] = f"缩量回踩 (量比 {ratios[-1]})"
        
    return features

# ==========================================
# 3. 组装经典 V2.2 格式快照
# ==========================================
def collect_v3_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tac_macro, strat_macro = get_macro_v3()
    
    dash_data = sh.worksheet("Dashboard").get_all_values()
    headers = dash_data[0]
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    strategic_archive = {"macro_matrix": strat_macro, "active_positions": [], "radar_graveyard": []}
    tactical_etfs, tactical_rules, exec_template = [], [], []

    # 解析宏观字符串
    macro_str_list = [f"* **{k}**: {v}" for k, v in tac_macro.items()]
    macro_str = "\n".join(macro_str_list)

    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[get_idx("基金名称")]
        proxy = re.search(r'\d{6}', row[get_idx("替身代码")] if get_idx("替身代码") != -1 else "").group(0) if re.search(r'\d{6}', row[get_idx("替身代码")] if get_idx("替身代码") != -1 else "") else ""
        rule = row[get_idx("战术纪律")] if get_idx("战术纪律") != -1 else ""
        
        # 提取持仓金额 (份额 * 最新净值)
        try:
            shares = float(re.sub(r'[^\d.]', '', row[get_idx("持有份额")]))
            nav = float(re.sub(r'[^\d.]', '', row[get_idx("最新净值")]))
            position_val = shares * nav
            pos_str = f"¥{position_val:,.0f}"
        except:
            pos_str = "未知"

        if rule: tactical_rules.append(f"- **{name}**：{rule}")
        # 强制 AI 把理由和证伪压缩
        exec_template.append(f"- **{name}**：[指令] | ￥[金额] | [理由：极简短句。证伪：限10字内短语]")
        
        if proxy:
            features = get_etf_5d_features(proxy)
            pct = features["today_pct"]
            tactical_etfs.append(f"* **{name} ({proxy})**: 盘中 {pct:+.2f}% | 量价: [{features['tactical_desc']}] | **当前持仓: {pos_str}**")
            strategic_archive["active_positions"].append({"name": name, "proxy": proxy, "today_pct": pct, "rule": rule})

    tactical_radar = []
    try:
        radar_data = sh.worksheet("雷达监控").get_all_values()
        for row in radar_data[1:]:
            if not row or not any(row): continue
            r_name = row[get_idx("板块名称")]
            r_proxy = re.search(r'\d{6}', row[get_idx("替身代码")]).group(0) if re.search(r'\d{6}', row[get_idx("替身代码")]) else ""
            r_trigger = row[get_idx("狙击触发条件")]
            if r_proxy:
                features = get_etf_5d_features(r_proxy)
                tactical_radar.append(f"* **{r_name} ({r_proxy})**: 盘中 {features['today_pct']:+.2f}% | 🎯扳机: {r_trigger}")
    except: pass

    etfs_str = "\n".join(tactical_etfs) if tactical_etfs else "暂无场内数据。"
    radar_str = "\n".join(tactical_radar) if tactical_radar else "雷达池未配置或为空。"
    
    # 完美复现 V2.2 数据快照排版
    md_prompt = f"""## 🌍 1. 全球宏观水位 (Macro)
{macro_str}

## 🎯 2. 核心场内替身盘中表现 (ETF Proxies)
{etfs_str}

## 📡 3. V2.0 雷达监控池 (4万备用金狩猎区)
{radar_str}

## 🧠 4. 账户记忆与底仓状态 (Account Memory)
* **可用现金弹药**: 约 4 万。下达雷达狙击指令时需统筹考虑。
"""
    return md_prompt, "\n".join(tactical_rules), "\n".join(exec_template), strategic_archive

# ==========================================
# 4. AI 经典排版决策大脑
# ==========================================
def ask_v3_tactical_agent(md_prompt: str, rules_str: str, exec_str: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V3.0 场外基金量化大脑 (Midday Agent)
当前是 14:45，请基于下方提供的数据快照进行决策。

【输入情报】：
{md_prompt}
【战术纪律】：
{rules_str}

# Output Format (必须严格遵守下方的排版与极简字数要求！)：

### 🌍 [宏观与主线诊断 (14:45)]
- **全球流动性**：(一句话概括美债与BTC流动性)
- **A股盘面判定**：(一句话概括A股资金主线，禁写废话)

### 🧠 [现役阵地概率推演与执行]
(用一段话，结合可用资金和宏观水位，概括总体战术重心：是全面进攻还是防守反击)

### 📝 [15:00 申赎执行单]
【核心警告：为防止微信截断，[理由]与[证伪]必须极度浓缩！证伪条件严禁写长句，只需写“破20日线”等几个字。】
{exec_str}

### 🎯 [雷达池量化监控 (4万备用金)]
(未触发则写“静默”。如有触发，简述理由)
    """
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(temperature=0.2)
    )
    return response.text

# ==========================================
# 5. 落盘归档与企微推送
# ==========================================
def archive_and_notify(md_prompt: str, ai_decision: str, strategic_json: dict):
    tz_bj = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_bj)
    time_prefix = now.strftime('%Y-%m-%d_%H%M')
    
    strategic_json["ai_tactical_decision"] = ai_decision  
    os.makedirs("logs", exist_ok=True)
    json_path = f"logs/{time_prefix}_Strategic.json"
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(strategic_json, f, ensure_ascii=False, indent=2)

    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if robot_key:
        full_content = f"{md_prompt}\n========================================\n\n{ai_decision}"
        
        # 安全截断，V2.2 格式较为冗长，设定在 3800 字节的极限边缘
        if len(full_content.encode('utf-8')) > 3800:
            full_content = full_content[:1150] + "\n\n...(字数超限截断，请留意核心指令)"
            
        payload = {
            "msgtype": "markdown", 
            "markdown": {
                "content": f"🚀 **V3.0 盘中决策 (经典再现版)**\n\n{full_content}"
            }
        }
        try:
            url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}"
            requests.post(url, json=payload)
            print("📡 企微推送成功！(经典格式已回归)")
        except Exception as e:
            print(f"❌ 企微网络异常: {e}")

if __name__ == "__main__":
    try:
        gc = get_gspread_client()
        md_prompt, rules_str, exec_str, strategic_json = collect_v3_intelligence(gc)
        ai_decision = ask_v3_tactical_agent(md_prompt, rules_str, exec_str)
        archive_and_notify(md_prompt, ai_decision, strategic_json)
    except Exception as e:
        print(f"❌ 运行失败: {e}")
        raise e
