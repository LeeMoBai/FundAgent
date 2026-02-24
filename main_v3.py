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
# 1. 抓取宏观数据 (包含 5 日序列与 DXY)
# ==========================================
def get_macro_v3() -> tuple:
    tactical_macro = {}
    strategic_macro = {}
    
    tickers = {
        "US10Y": "^TNX", 
        "BTC": "BTC-USD", 
        "KOSPI": "^KS11", 
        "XAU_USD": "GC=F", 
        "NQmain": "NQ=F", 
        "XBI": "XBI",
        "DXY": "DX-Y.NYB"  # 🎯 V3.0 新增美元指数
    }
    
    for key, symbol in tickers.items():
        try:
            data = yf.Ticker(symbol).history(period="5d")
            if len(data) >= 2:
                closes = data['Close'].tolist()
                current = closes[-1]
                prev_close = closes[-2]
                pct_chg = ((current - prev_close) / prev_close) * 100
                
                # 🔴 Tactical (给 AI 和微信看的：极简当前切片)
                if key == "BTC": tactical_macro[key] = f"${current:,.0f} ({pct_chg:+.2f}%)"
                elif key == "XAU_USD": tactical_macro[key] = f"${current:,.2f} ({pct_chg:+.2f}%)"
                else: tactical_macro[key] = f"{current:.3f} ({pct_chg:+.2f}%)"
                
                # 🔵 Strategic (存入 JSON 的：完整 5 日序列)
                strategic_macro[key] = {
                    "current": current,
                    "daily_pct": pct_chg,
                    "5d_trend": [round(x, 3) for x in closes]
                }
            else:
                tactical_macro[key] = "N/A"
                strategic_macro[key] = {"error": " insufficient data"}
        except:
            tactical_macro[key] = "拉取失败"
            strategic_macro[key] = {"error": "fetch failed"}
            
    return tactical_macro, strategic_macro

# ==========================================
# 2. 抓取 ETF 5日量价序列
# ==========================================
def get_etf_5d_features(proxy_code: str) -> dict:
    features = {
        "is_abnormal_vol": False,
        "tactical_desc": "温和平量",
        "5d_prices": [],
        "5d_vol_ratios": [],
        "ma250_dist": 0.0
    }
    try:
        hist_df = ak.fund_etf_hist_em(symbol=proxy_code, period="daily")
        if len(hist_df) >= 2:
            recent_5 = hist_df.tail(6) # 取6天为了算5个量比
            vols = recent_5['成交额'].tolist()
            prices = recent_5['收盘'].tolist()
            
            ratios = []
            for i in range(1, len(vols)):
                ratio = vols[i] / vols[i-1] if vols[i-1] > 0 else 1.0
                ratios.append(round(ratio, 2))
                
            features["5d_prices"] = prices[1:]
            features["5d_vol_ratios"] = ratios
            
            # 判断今天是否异动 (量比 > 1.2 或 < 0.8)
            today_ratio = ratios[-1]
            if today_ratio > 1.2:
                features["is_abnormal_vol"] = True
                features["tactical_desc"] = f"异常放量 (量比 {today_ratio})"
            elif today_ratio < 0.8:
                features["is_abnormal_vol"] = True
                features["tactical_desc"] = f"极致缩量 (量比 {today_ratio})"
                
        if len(hist_df) >= 20:
            ma250 = hist_df.tail(250)['收盘'].mean()
            current = hist_df.iloc[-1]['收盘']
            features["ma250_dist"] = round(((current - ma250) / ma250) * 100, 2)
            
    except:
        pass
    return features

# ==========================================
# 3. 双轨情报组装 (MVP vs Data Lake)
# ==========================================
def collect_v3_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tac_macro, strat_macro = get_macro_v3()
    
    print("   [+] 拉取 ETF 快照与 5 日时序数据...")
    try: etf_spot = ak.fund_etf_spot_em()
    except: etf_spot = pd.DataFrame()

    dash_data = sh.worksheet("Dashboard").get_all_values()
    headers = dash_data[0]
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    # 战略级 JSON 底稿容器
    strategic_archive = {
        "macro_matrix": strat_macro,
        "active_positions": [],
        "radar_graveyard": []
    }
    
    # 战术级 Markdown 容器
    tactical_etfs = []
    tactical_rules = []
    exec_template = []

    # 1. 处理现役持仓
    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[get_idx("基金名称")]
        proxy_raw = row[get_idx("替身代码")]
        proxy = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        rule = row[get_idx("战术纪律")] if get_idx("战术纪律") != -1 else ""
        
        if rule: tactical_rules.append(f"- **【{name}】**：{rule}")
        exec_template.append(f"- **{name}**：[指令] | ￥[金额] | [客观理由与置信度]")
        
        if proxy and not etf_spot.empty:
            match = etf_spot[etf_spot["代码"] == proxy]
            if not match.empty:
                pct = match.iloc[0]['涨跌幅']
                features = get_etf_5d_features(proxy)
                
                # 🔴 战术级：只给当前切片
                tactical_etfs.append(f"* **{name}**: 盘中 {pct:+.2f}% | 量价: [{features['tactical_desc']}]")
                
                # 🔵 战略级：记录连续动作
                strategic_archive["active_positions"].append({
                    "name": name,
                    "proxy": proxy,
                    "today_pct": pct,
                    "5d_prices": features["5d_prices"],
                    "5d_vol_ratios": features["5d_vol_ratios"],
                    "rule_active": rule
                })

    # 2. 处理雷达池 (动态隐身逻辑)
    print("   [+] 扫描雷达池 (触发过滤机制)...")
    tactical_radar = []
    try:
        radar_data = sh.worksheet("雷达监控").get_all_values()
        r_headers = radar_data[0]
        
        for row in radar_data[1:]:
            if not row or not any(row): continue
            r_name = row[get_idx("板块名称")]
            r_proxy_raw = row[get_idx("替身代码")]
            r_proxy = re.search(r'\d{6}', r_proxy_raw).group(0) if re.search(r'\d{6}', r_proxy_raw) else ""
            r_trigger = row[get_idx("狙击触发条件")]
            
            if r_proxy and not etf_spot.empty:
                match = etf_spot[etf_spot["代码"] == r_proxy]
                if not match.empty:
                    pct = match.iloc[0]['涨跌幅']
                    features = get_etf_5d_features(r_proxy)
                    
                    # 🔴 战术级过滤：只有大跌(<-1%)、距年线近(<3%)或异动放量，才送去给AI看。没触发的彻底静默！
                    is_triggered = pct < -1.0 or abs(features["ma250_dist"]) < 3.0 or features["is_abnormal_vol"]
                    
                    if is_triggered:
                        tactical_radar.append(f"* **{r_name}**: 盘中 {pct:+.2f}% | {features['tactical_desc']} | 🎯 扳机: {r_trigger}")
                    
                    # 🔵 战略级：全量打入雷达坟场，留给月度复盘算踏空成本
                    strategic_archive["radar_graveyard"].append({
                        "name": r_name,
                        "today_pct": pct,
                        "ma250_dist": features["ma250_dist"],
                        "5d_vol_ratios": features["5d_vol_ratios"],
                        "was_triggered_today": is_triggered
                    })
    except: pass

    # 组装极简 Prompt
    md_prompt = f"""## 🌍 1. 核心宏观锚点
US10Y: {tac_macro.get('US10Y')} | BTC: {tac_macro.get('BTC')} | KOSPI: {tac_macro.get('KOSPI')} | NQmain: {tac_macro.get('NQmain')}

## 🎯 2. 现役阵地切片
{"\n".join(tactical_etfs)}

## 📡 3. 异动雷达 (仅显示疑似触发标的)
{"\n".join(tactical_radar) if tactical_radar else "全量安全，无资产触发预警。静默。"}
"""
    return md_prompt, "\n".join(tactical_rules), "\n".join(exec_template), strategic_archive

# ==========================================
# 4. AI 极简战术大脑
# ==========================================
def ask_v3_tactical_agent(md_prompt: str, rules_str: str, exec_str: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V3.0 战术突击手 (Tactical Agent)
当前14:45，基于下方极其精简的切片数据，输出冷酷执行单。禁止写长篇大论分析。

【极简盘面】：
{md_prompt}
【底线纪律】：
{rules_str}

# Output Format:
### ⚔️ [宏观定调]
(一句话：说明宏观流动性对A股的压制/支撑)
### 📝 [15:00 申赎执行单]
{exec_str}
### 🎯 [雷达池动作]
(如果有触发，明确买入指令；如显示静默，则回复“雷达区无警报，备用金静默”。)
    """
    return client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(temperature=0.1)
    ).text

# ==========================================
# 5. 落盘归档与企微推送
# ==========================================
def archive_and_notify(ai_decision: str, strategic_json: dict):
    tz_bj = pytz.timezone('Asia/Shanghai')
    now = datetime.datetime.now(tz_bj)
    time_prefix = now.strftime('%Y-%m-%d_%H%M')
    
    # 1. 写入战略级 JSON
    strategic_json["ai_tactical_decision"] = ai_decision  # 把 AI 的判决也一并存入底稿
    os.makedirs("logs", exist_ok=True)
    json_path = f"logs/{time_prefix}_Strategic.json"
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(strategic_json, f, ensure_ascii=False, indent=2)
    print(f"📦 战略级 JSON 已入库: {json_path}")

    # 2. 推送企业微信
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if robot_key:
        payload = {"msgtype": "markdown", "markdown": {"content": f"<font color=\"warning\">**🚀 V3.0 战术决策 (影分身测试)**</font>\n\n{ai_decision}"}}
        requests.post(f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}", json=payload)
        print("📡 企微推送成功！")

# ==========================================
# 主流程
# ==========================================
if __name__ == "__main__":
    print("🚀 [V3.0 Shadow Run] 启动战术/战略双轨测试引擎...")
    gc = get_gspread_client()
    
    md_prompt, rules_str, exec_str, strategic_json = collect_v3_intelligence(gc)
    
    print("⏳ 唤醒战术大脑计算指令...")
    ai_decision = ask_v3_tactical_agent(md_prompt, rules_str, exec_str)
    
    print("⏳ 执行 JSON 落盘与微信分发...")
    archive_and_notify(ai_decision, strategic_json)
    
    print("🎉 V3.0 影子测试运行完毕！")
