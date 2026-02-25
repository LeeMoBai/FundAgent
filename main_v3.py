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
        "DXY": "DX-Y.NYB"  
    }
    
    for key, symbol in tickers.items():
        try:
            data = yf.Ticker(symbol).history(period="5d")
            if len(data) >= 2:
                closes = data['Close'].tolist()
                current = closes[-1]
                prev_close = closes[-2]
                pct_chg = ((current - prev_close) / prev_close) * 100
                
                if key == "BTC": tactical_macro[key] = f"${current:,.0f} ({pct_chg:+.2f}%)"
                elif key == "XAU_USD": tactical_macro[key] = f"${current:,.2f} ({pct_chg:+.2f}%)"
                else: tactical_macro[key] = f"{current:.3f} ({pct_chg:+.2f}%)"
                
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
# 2. 抓取 ETF 5日量价特征
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
            recent_5 = hist_df.tail(6) 
            vols = recent_5['成交额'].tolist()
            prices = recent_5['收盘'].tolist()
            
            ratios = []
            for i in range(1, len(vols)):
                ratio = vols[i] / vols[i-1] if vols[i-1] > 0 else 1.0
                ratios.append(round(ratio, 2))
                
            features["5d_prices"] = prices[1:]
            features["5d_vol_ratios"] = ratios
            
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
# 3. 组装双轨情报 (重塑经典排版)
# ==========================================
def collect_v3_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tac_macro, strat_macro = get_macro_v3()
    
    print("   [+] 拉取 ETF 快照与时序数据...")
    try: etf_spot = ak.fund_etf_spot_em()
    except: etf_spot = pd.DataFrame()

    dash_data = sh.worksheet("Dashboard").get_all_values()
    headers = dash_data[0]
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    strategic_archive = {"macro_matrix": strat_macro, "active_positions": [], "radar_graveyard": []}
    tactical_etfs = []
    tactical_rules = []
    exec_template = []

    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[get_idx("基金名称")]
        proxy_raw = row[get_idx("替身代码")]
        proxy = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        rule = row[get_idx("战术纪律")] if get_idx("战术纪律") != -1 else ""
        
        if rule: tactical_rules.append(f"- **【{name}】**：{rule}")
        exec_template.append(f"- **{name}**：[指令] | ￥[金额] | [理由与置信度]")
        
        if proxy and not etf_spot.empty:
            match = etf_spot[etf_spot["代码"] == proxy]
            if not match.empty:
                pct = match.iloc[0]['涨跌幅']
                features = get_etf_5d_features(proxy)
                tactical_etfs.append(f"* **{name} ({proxy})**: 盘中 {pct:+.2f}% | 量价: [{features['tactical_desc']}]")
                strategic_archive["active_positions"].append({
                    "name": name, "proxy": proxy, "today_pct": pct,
                    "5d_prices": features["5d_prices"], "5d_vol_ratios": features["5d_vol_ratios"],
                    "rule_active": rule
                })

    print("   [+] 扫描雷达池...")
    tactical_radar = []
    try:
        radar_data = sh.worksheet("雷达监控").get_all_values()
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
                    is_triggered = pct < -1.0 or abs(features["ma250_dist"]) < 3.0 or features["is_abnormal_vol"]
                    if is_triggered:
                        tactical_radar.append(f"* **{r_name}**: 盘中 {pct:+.2f}% | {features['tactical_desc']} | 🎯 扳机: {r_trigger}")
                    strategic_archive["radar_graveyard"].append({
                        "name": r_name, "today_pct": pct, "ma250_dist": features["ma250_dist"],
                        "5d_vol_ratios": features["5d_vol_ratios"], "was_triggered_today": is_triggered
                    })
    except: pass

    etfs_str = "\n".join(tactical_etfs)
    radar_str = "\n".join(tactical_radar) if tactical_radar else "全量安全，无资产触发预警。静默。"
    
    # 🎯 恢复原有的数据快照排版
    md_prompt = f"""## 🌍 1. 全球宏观水位
US10Y: {tac_macro.get('US10Y')} | BTC: {tac_macro.get('BTC')} | KOSPI: {tac_macro.get('KOSPI')} | DXY: {tac_macro.get('DXY')}

## 🎯 2. 核心场内表现
{etfs_str}

## 📡 3. 异动雷达
{radar_str}
"""
    rules_out = "\n".join(tactical_rules)
    exec_out = "\n".join(exec_template)
    
    return md_prompt, rules_out, exec_out, strategic_archive

# ==========================================
# 4. AI 深度决策大脑
# ==========================================
def ask_v3_tactical_agent(md_prompt: str, rules_str: str, exec_str: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V3.0 场外基金量化大脑 (Midday Agent)
当前14:45，基于下方情报输出决策。
【核心指令】：请务必结合全球宏观市场（如韩国股市、比特币流动性）、科技巨头财报细节（如资本开支流向）以及资金高低切换逻辑进行深度分析，不要只看表面涨跌。

【输入情报】：
{md_prompt}
【战术纪律】：
{rules_str}

# Output Format:
### 🌍 [宏观与主线诊断 (14:45)]
(结合宏观与量价进行深度发散分析，判定资金真实意图)
### 🧠 [现役阵地概率推演与执行]
(详细推演底层逻辑与证伪条件)
### 📝 [15:00 申赎执行单]
{exec_str}
### 🎯 [雷达池量化监控]
(如果有触发，明确买入指令；无则静默)
    """
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(temperature=0.2)
    )
    return response.text

# ==========================================
# 5. 落盘归档与企微拼接推送
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
    print(f"📦 战略级 JSON 已入库: {json_path}")

    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if robot_key:
        # 🎯 核心修改：把 md_prompt（数据快照）和 ai_decision（推演结论）强力拼接在一起发给微信
        full_content = f"{md_prompt}\n---\n{ai_decision}"
        
        # 增加长度保护，防止内容过长微信吞字
        if len(full_content) > 3900:
            full_content = full_content[:3900] + "\n\n...(部分内容因长度截断)"
            
        payload = {
            "msgtype": "markdown", 
            "markdown": {
                "content": f"<font color='warning'>**🚀 V3.0 盘中决策 (完整推演版)**</font>\n\n{full_content}"
            }
        }
        try:
            url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}"
            requests.post(url, json=payload)
            print("📡 企微推送成功！(含完整数据与深度推演)")
        except Exception as e:
            print(f"❌ 企微推送网络异常: {e}")

# ==========================================
# 主流程
# ==========================================
if __name__ == "__main__":
    print("🚀 [V3.0 Shadow Run] 启动满血版双轨测试引擎...")
    try:
        gc = get_gspread_client()
        md_prompt, rules_str, exec_str, strategic_json = collect_v3_intelligence(gc)
        
        print("⏳ 唤醒大脑进行深度推演计算...")
        ai_decision = ask_v3_tactical_agent(md_prompt, rules_str, exec_str)
        
        print("⏳ 执行 JSON 落盘与微信全量分发...")
        # 🎯 注意这里多传了一个参数 md_prompt 进去
        archive_and_notify(md_prompt, ai_decision, strategic_json)
        
        print("🎉 满血版影子测试运行完毕！")
    except Exception as e:
        print(f"❌ 运行失败: {e}")
        raise e
