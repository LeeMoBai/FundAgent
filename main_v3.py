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
# 1. 抓取宏观数据
# ==========================================
def get_macro_v3() -> tuple:
    tactical_macro = {}
    strategic_macro = {}
    tickers = {"US10Y": "^TNX", "BTC": "BTC-USD", "KOSPI": "^KS11", "XAU_USD": "GC=F", "NQmain": "NQ=F", "XBI": "XBI", "DXY": "DX-Y.NYB"}
    
    for key, symbol in tickers.items():
        try:
            data = yf.Ticker(symbol).history(period="5d")
            if len(data) >= 2:
                closes = data['Close'].tolist()
                pct_chg = ((closes[-1] - closes[-2]) / closes[-2]) * 100
                
                if key == "BTC": tactical_macro[key] = f"${closes[-1]:,.0f}({pct_chg:+.2f}%)"
                elif key == "XAU_USD": tactical_macro[key] = f"${closes[-1]:,.0f}({pct_chg:+.2f}%)"
                else: tactical_macro[key] = f"{closes[-1]:.2f}({pct_chg:+.2f}%)"
                
                strategic_macro[key] = {"current": closes[-1], "daily_pct": pct_chg, "5d_trend": [round(x, 3) for x in closes]}
            else:
                tactical_macro[key] = "N/A"
                strategic_macro[key] = {"error": " insufficient data"}
        except:
            tactical_macro[key] = "超时"
            strategic_macro[key] = {"error": "fetch failed"}
            
    return tactical_macro, strategic_macro

# ==========================================
# 2. 抓取 ETF 5日量价 (引入 YFinance 容灾引擎)
# ==========================================
def get_etf_5d_features(proxy_code: str) -> dict:
    features = {"today_pct": 0.0, "is_abnormal_vol": False, "tactical_desc": "温和平量", "5d_prices": [], "5d_vol_ratios": [], "ma250_dist": 0.0}
    
    try:
        # 引擎 1：尝试 AkShare
        hist_df = ak.fund_etf_hist_em(symbol=proxy_code, period="daily")
        if len(hist_df) < 2: raise ValueError("AkShare 数据为空")
        vols = hist_df.tail(6)['成交额'].tolist()
        prices = hist_df.tail(6)['收盘'].tolist()
        if len(hist_df) >= 20:
            ma250 = hist_df.tail(250)['收盘'].mean()
            features["ma250_dist"] = round(((prices[-1] - ma250) / ma250) * 100, 2)
            
    except Exception:
        # 引擎 2：若 AkShare 被拦截，无缝回退到 YFinance
        try:
            suffix = ".SS" if proxy_code.startswith("5") else ".SZ"
            df_yf = yf.Ticker(f"{proxy_code}{suffix}").history(period="6d")
            if len(df_yf) < 2: raise ValueError("YF 数据为空")
            vols = df_yf['Volume'].tolist()
            prices = df_yf['Close'].tolist()
        except Exception:
            features["tactical_desc"] = "接口双重熔断"
            return features

    # 统一计算逻辑
    pct = ((prices[-1] - prices[-2]) / prices[-2]) * 100
    features["today_pct"] = round(pct, 2)
    
    ratios = []
    for i in range(1, len(vols)):
        ratio = vols[i] / vols[i-1] if vols[i-1] > 0 else 1.0
        ratios.append(round(ratio, 2))
        
    features["5d_prices"] = [round(p, 3) for p in prices[1:]]
    features["5d_vol_ratios"] = ratios
    
    today_ratio = ratios[-1]
    if today_ratio > 1.2:
        features["is_abnormal_vol"] = True
        features["tactical_desc"] = f"放量(量比{today_ratio})"
    elif today_ratio < 0.8:
        features["is_abnormal_vol"] = True
        features["tactical_desc"] = f"缩量(量比{today_ratio})"
        
    return features

# ==========================================
# 3. 组装双轨情报 (极限瘦身排版)
# ==========================================
def collect_v3_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tac_macro, strat_macro = get_macro_v3()
    
    print("   [+] 拉取双引擎 ETF 数据...")
    dash_data = sh.worksheet("Dashboard").get_all_values()
    headers = dash_data[0]
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    strategic_archive = {"macro_matrix": strat_macro, "active_positions": [], "radar_graveyard": []}
    tactical_etfs, tactical_rules, exec_template = [], [], []

    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[get_idx("基金名称")]
        proxy = re.search(r'\d{6}', row[get_idx("替身代码")] if get_idx("替身代码") != -1 else "").group(0) if re.search(r'\d{6}', row[get_idx("替身代码")] if get_idx("替身代码") != -1 else "") else ""
        rule = row[get_idx("战术纪律")] if get_idx("战术纪律") != -1 else ""
        
        if rule: tactical_rules.append(f"- **{name}**：{rule}")
        exec_template.append(f"- **{name}**：[买/卖/锁仓] | ￥[金额] | [极简理由]")
        
        if proxy:
            features = get_etf_5d_features(proxy)
            pct = features["today_pct"]
            # 极简排版，节省微信字节
            tactical_etfs.append(f"* {name}({proxy}): {pct:+.2f}% | {features['tactical_desc']}")
            strategic_archive["active_positions"].append({
                "name": name, "proxy": proxy, "today_pct": pct,
                "5d_vol_ratios": features["5d_vol_ratios"], "rule_active": rule
            })

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
                pct = features["today_pct"]
                is_triggered = pct < -1.0 or abs(features["ma250_dist"]) < 3.0 or features["is_abnormal_vol"]
                if is_triggered:
                    tactical_radar.append(f"* {r_name}: {pct:+.2f}% | 🎯扳机: {r_trigger}")
    except: pass

    etfs_str = "\n".join(tactical_etfs) if tactical_etfs else "无数据"
    radar_str = "\n".join(tactical_radar) if tactical_radar else "备用金静默，无触发标的。"
    
    # 极简前置快照
    md_prompt = f"""### 🌍 宏观与场内快照
US10Y:{tac_macro.get('US10Y')} | BTC:{tac_macro.get('BTC')} | KOSPI:{tac_macro.get('KOSPI')}
{etfs_str}
**雷达**: {radar_str}
"""
    return md_prompt, "\n".join(tactical_rules), "\n".join(exec_template), strategic_archive

# ==========================================
# 4. AI 深度决策大脑 (强制字数压缩)
# ==========================================
def ask_v3_tactical_agent(md_prompt: str, rules_str: str, exec_str: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V3.0 场外基金量化大脑 (Midday Agent)
请结合宏观市场(如KOSPI、BTC)及巨头财报流向进行深度推演。

【输入情报】：
{md_prompt}
【战术纪律】：
{rules_str}

# Output Format (严格按此格式输出)：
### 📝 [15:00 申赎执行单]
{exec_str}

### 🧠 [宏观主线与阵地推演]
【字数红线：此部分包含推演和逻辑，必须极其精炼，总字数绝对不准超过400字，否则系统将熔断！】
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
    print(f"📦 战略级 JSON 已入库")

    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if robot_key:
        full_content = f"{md_prompt}\n---\n{ai_decision}"
        
        # 安全截断阈值，3800 字节在微信 4096 限制内游刃有余
        if len(full_content.encode('utf-8')) > 3800:
            full_content = full_content[:1150] + "\n\n[字数超限，尾部推演已截断，请查阅本地JSON]"
            
        payload = {
            "msgtype": "markdown", 
            "markdown": {
                "content": f"🚀 **V3.0 战术决断**\n\n{full_content}"
            }
        }
        try:
            url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}"
            res = requests.post(url, json=payload)
            if res.json().get("errcode") == 0:
                print("📡 企微推送成功！(双引擎数据 + 压缩排版)")
            else:
                print(f"❌ 企微拒绝发送！错误码: {res.json().get('errcode')}")
        except Exception as e:
            print(f"❌ 企微网络异常: {e}")

if __name__ == "__main__":
    print("🚀 [V3.0 Shadow Run] 启动容灾双引擎...")
    try:
        gc = get_gspread_client()
        md_prompt, rules_str, exec_str, strategic_json = collect_v3_intelligence(gc)
        ai_decision = ask_v3_tactical_agent(md_prompt, rules_str, exec_str)
        archive_and_notify(md_prompt, ai_decision, strategic_json)
        print("🎉 任务运行完毕！")
    except Exception as e:
        print(f"❌ 运行失败: {e}")
        raise e
