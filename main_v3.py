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
import concurrent.futures

# ==========================================
# 0. 战术熔断器 (Tactical Timeout)
# ==========================================
def fetch_with_timeout(func, timeout_sec, *args, **kwargs):
    """强制熔断机制：防止任何第三方 API 假死导致系统卡死"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            raise TimeoutError("接口假死，已强制战术熔断！")

def get_yf_history(symbol, period="5d"):
    """封装 YFinance 以便穿透熔断器"""
    return yf.Ticker(symbol).history(period=period)

def get_gspread_client():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("缺失 GCP_SERVICE_ACCOUNT")
    return gspread.service_account_from_dict(json.loads(creds_json))

# ==========================================
# 1. 抓取宏观数据 (带 5 秒熔断)
# ==========================================
def get_macro_v3() -> tuple:
    tactical_macro = {}
    strategic_macro = {}
    tickers_map = {
        "美债(US10Y)": "^TNX", 
        "BTC": "BTC-USD", 
        "KOSPI": "^KS11", 
        "黄金(XAU)": "GC=F", 
        "纳指(NQ)": "NQ=F", 
        "XBI": "XBI"
    }
    
    for display_name, symbol in tickers_map.items():
        try:
            # 🎯 给 YFinance 加上 5 秒强制熔断
            data = fetch_with_timeout(get_yf_history, 5, symbol, "5d")
            if len(data) >= 2:
                closes = data['Close'].tolist()
                pct_chg = ((closes[-1] - closes[-2]) / closes[-2]) * 100
                
                if "BTC" in display_name or "XAU" in display_name:
                    tactical_macro[display_name] = f"${closes[-1]:,.0f}({pct_chg:+.2f}%)"
                elif "US10Y" in display_name:
                    tactical_macro[display_name] = f"{closes[-1]:.2f}%({pct_chg:+.2f}%)"
                else:
                    tactical_macro[display_name] = f"{closes[-1]:.0f}({pct_chg:+.2f}%)"
                
                strategic_macro[symbol] = {"current": closes[-1], "daily_pct": pct_chg, "5d_trend": [round(x, 3) for x in closes]}
            else:
                tactical_macro[display_name] = "N/A"
        except:
            tactical_macro[display_name] = "盲区"
            
    return tactical_macro, strategic_macro

# ==========================================
# 2. 抓取 ETF (双引擎容灾 + 5秒熔断)
# ==========================================
def get_realtime_etf_features(proxy_code: str, spot_df: pd.DataFrame) -> dict:
    features = {"today_pct": 0.0, "tactical_desc": "量价盲区", "5d_vol_ratios": [], "ma250_dist": 0.0}
    pct_found = False
    
    if not spot_df.empty:
        match = spot_df[spot_df["代码"] == proxy_code]
        if not match.empty:
            features["today_pct"] = round(float(match.iloc[0]['涨跌幅']), 2)
            pct_found = True
            
    vols, prices = [], []
    
    # 🎯 AkShare 引擎 (限时 5 秒)
    try:
        hist_df = fetch_with_timeout(ak.fund_etf_hist_em, 5, symbol=proxy_code, period="daily")
        if len(hist_df) >= 2:
            vols = hist_df.tail(6)['成交额'].tolist()
            prices = hist_df.tail(6)['收盘'].tolist()
            if len(hist_df) >= 20:
                ma250 = hist_df.tail(250)['收盘'].mean()
                features["ma250_dist"] = round(((hist_df.iloc[-1]['收盘'] - ma250) / ma250) * 100, 2)
            if not pct_found:
                features["today_pct"] = round(((prices[-1] - prices[-2]) / prices[-2]) * 100, 2)
                pct_found = True
    except:
        pass 

    # 🎯 YFinance 引擎接管 (限时 5 秒)
    if not vols or not prices:
        try:
            suffix = ".SS" if proxy_code.startswith("5") else ".SZ"
            df_yf = fetch_with_timeout(get_yf_history, 5, f"{proxy_code}{suffix}", "6d")
            if len(df_yf) >= 2:
                vols = df_yf['Volume'].tolist()
                prices = df_yf['Close'].tolist()
                if not pct_found:
                    features["today_pct"] = round(((prices[-1] - prices[-2]) / prices[-2]) * 100, 2)
        except:
            pass

    # 计算量比
    if vols and len(vols) >= 2:
        ratios = [round(vols[i]/vols[i-1], 2) if vols[i-1]>0 else 1.0 for i in range(1, len(vols))]
        features["5d_vol_ratios"] = ratios
        last_ratio = ratios[-1]
        
        if last_ratio > 1.2: features["tactical_desc"] = f"放量({last_ratio})"
        elif last_ratio < 0.8: features["tactical_desc"] = f"缩量({last_ratio})"
        else: features["tactical_desc"] = f"平量({last_ratio})"
            
    return features

# ==========================================
# 3. 组装极度瘦身的经典快照
# ==========================================
def collect_v3_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tac_macro, strat_macro = get_macro_v3()
    
    print("   [+] 启动双引擎穿透式抓取 (带超时熔断)...")
    try: 
        # 🎯 全市场盘口抓取限时 8 秒，绝不卡死
        etf_spot = fetch_with_timeout(ak.fund_etf_spot_em, 8)
    except: 
        etf_spot = pd.DataFrame()
    
    dash_data = sh.worksheet("Dashboard").get_all_values()
    headers = dash_data[0]
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    strategic_archive = {"macro_matrix": strat_macro, "active_positions": [], "radar_graveyard": []}
    tactical_etfs, tactical_rules, exec_template = [], [], []

    macro_str = " | ".join([f"{k}:{v}" for k, v in tac_macro.items()])

    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[get_idx("基金名称")]
        proxy = re.search(r'\d{6}', row[get_idx("替身代码")] if get_idx("替身代码") != -1 else "").group(0) if re.search(r'\d{6}', row[get_idx("替身代码")] if get_idx("替身代码") != -1 else "") else ""
        rule = row[get_idx("战术纪律")] if get_idx("战术纪律") != -1 else ""
        
        try:
            shares = float(re.sub(r'[^\d.]', '', row[get_idx("持有份额")]))
            nav = float(re.sub(r'[^\d.]', '', row[get_idx("最新净值")]))
            val = shares * nav
            pos_str = f"¥{val/1000:.1f}k" if val > 0 else "¥0"
        except:
            pos_str = "未知"

        if rule: tactical_rules.append(f"- **{name}**：{rule}")
        exec_template.append(f"- **{name}**：[指令] | ￥[金额] | [理由(限15字)。[证伪]:限8字]")
        
        if proxy:
            features = get_realtime_etf_features(proxy, etf_spot)
            pct = features["today_pct"]
            tactical_etfs.append(f"* **{name}**({proxy}): {pct:+.2f}% | 仓:{pos_str} | {features['tactical_desc']}")
            strategic_archive["active_positions"].append({"name": name, "proxy": proxy, "today_pct": pct})

    tactical_radar = []
    try:
        radar_data = sh.worksheet("雷达监控").get_all_values()
        for row in radar_data[1:]:
            if not row or not any(row): continue
            r_name = row[get_idx("板块名称")]
            r_proxy = re.search(r'\d{6}', row[get_idx("替身代码")]).group(0) if re.search(r'\d{6}', row[get_idx("替身代码")]) else ""
            r_trigger = row[get_idx("狙击触发条件")]
            if r_proxy:
                features = get_realtime_etf_features(r_proxy, etf_spot)
                tactical_radar.append(f"* **{r_name}**: {features['today_pct']:+.2f}% | 🎯扳机: {r_trigger}")
    except: pass

    etfs_str = "\n".join(tactical_etfs) if tactical_etfs else "暂无场内数据。"
    radar_str = "\n".join(tactical_radar) if tactical_radar else "空。"
    
    md_prompt = f"""## 🌍 宏观水位
{macro_str}

## 🎯 场内替身盘口
{etfs_str}

## 📡 雷达池 (4万备用金)
{radar_str}
"""
    return md_prompt, "\n".join(tactical_rules), "\n".join(exec_template), strategic_archive

# ==========================================
# 4. AI 高压字数限制大脑
# ==========================================
def ask_v3_tactical_agent(md_prompt: str, rules_str: str, exec_str: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V3.0 场外基金量化大脑 (Midday Agent)
当前14:45，基于数据极速决断。

【输入情报】：
{md_prompt}
【战术纪律】：
{rules_str}

【高压红线】：为了防范微信截断，你的输出总字数绝对不准超过 400 字！[理由]必须控制在15个字以内！[证伪]必须控制在8个字以内（如：破20日线）！

# Output Format：

### 🌍 [宏观主线诊断]
- 流动性：(极简，限20字)
- A股主线：(极简，限30字)

### 🧠 [阵地推演]
(极简概括战术重心，限50字)

### 📝 [15:00 执行单]
{exec_str}
    """
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(temperature=0.1)
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
        full_content = f"{md_prompt}\n====================\n\n{ai_decision}"
        
        if len(full_content.encode('utf-8')) > 3900:
            full_content = full_content[:1250] + "\n\n...(字数触及上限，安全截断)"
            
        payload = {
            "msgtype": "markdown", 
            "markdown": {
                "content": f"🚀 **V3.0 盘中决策 (防假死突围版)**\n\n{full_content}"
            }
        }
        try:
            url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}"
            requests.post(url, json=payload)
            print("📡 推送成功！")
        except Exception as e:
            print(f"❌ 网络异常: {e}")

if __name__ == "__main__":
    try:
        gc = get_gspread_client()
        md_prompt, rules_str, exec_str, strategic_json = collect_v3_intelligence(gc)
        ai_decision = ask_v3_tactical_agent(md_prompt, rules_str, exec_str)
        archive_and_notify(md_prompt, ai_decision, strategic_json)
    except Exception as e:
        print(f"❌ 运行失败: {e}")
        raise e
