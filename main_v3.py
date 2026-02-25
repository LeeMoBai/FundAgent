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
# 0. 战术熔断器
# ==========================================
def fetch_with_timeout(func, timeout_sec, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            raise TimeoutError("接口假死，已强制战术熔断！")

def get_yf_history(symbol, period="5d"):
    return yf.Ticker(symbol).history(period=period)

def get_gspread_client():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("缺失 GCP_SERVICE_ACCOUNT")
    return gspread.service_account_from_dict(json.loads(creds_json))

# ==========================================
# 1. 抓取外围宏观数据 (包含 QDII 隐藏锚点)
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
        "XBI": "XBI",
        "IXIC_Hidden": "^IXIC" 
    }
    
    for display_name, symbol in tickers_map.items():
        try:
            data = fetch_with_timeout(get_yf_history, 4, symbol, "5d")
            if len(data) >= 2:
                closes = data['Close'].tolist()
                pct_chg = ((closes[-1] - closes[-2]) / closes[-2]) * 100
                
                if display_name != "IXIC_Hidden":
                    if "BTC" in display_name or "XAU" in display_name:
                        tactical_macro[display_name] = f"${closes[-1]:,.0f}({pct_chg:+.2f}%)"
                    elif "US10Y" in display_name:
                        tactical_macro[display_name] = f"{closes[-1]:.2f}%({pct_chg:+.2f}%)"
                    else:
                        tactical_macro[display_name] = f"{closes[-1]:.0f}({pct_chg:+.2f}%)"
                        
                strategic_macro[symbol] = {"current": closes[-1], "daily_pct": pct_chg}
            else:
                if display_name != "IXIC_Hidden": tactical_macro[display_name] = "N/A"
        except:
            if display_name != "IXIC_Hidden": tactical_macro[display_name] = "盲区"
            
    return tactical_macro, strategic_macro

# ==========================================
# 2. 抓取 ETF (引入 V4.0 定量均线计算)
# ==========================================
def get_realtime_etf_features(proxy_code: str) -> dict:
    features = {
        "today_pct": 0.0, 
        "tactical_desc": "无量价", 
        "ma20_dist": 0.0,   # 🎯 V4.0 核心：距离20日线的乖离率
        "ma60_dist": 0.0    # 🎯 V4.0 核心：距离60日线的乖离率
    }
    
    # 步骤 1：腾讯极速 API 抓盘口涨跌
    try:
        prefix = "sh" if proxy_code.startswith("5") else "sz"
        url = f"http://qt.gtimg.cn/q={prefix}{proxy_code}"
        resp = fetch_with_timeout(requests.get, 3, url)
        data = resp.text.split('~')
        if len(data) > 32:
            features["today_pct"] = round(float(data[32]), 2)
    except:
        pass 

    # 步骤 2：抓历史算量比与均线
    vols = []
    for _ in range(2):
        try:
            hist_df = fetch_with_timeout(ak.fund_etf_hist_em, 5, symbol=proxy_code, period="daily")
            if len(hist_df) >= 2:
                vols = hist_df.tail(6)['成交额'].tolist()
                closes = hist_df['收盘'].tolist()
                current_c = closes[-1]
                
                # 🎯 V4.0 机器苦力：计算 MA20 与 MA60 的实时乖离
                if len(closes) >= 20:
                    ma20 = sum(closes[-20:]) / 20.0
                    features["ma20_dist"] = round(((current_c - ma20) / ma20) * 100, 2)
                if len(closes) >= 60:
                    ma60 = sum(closes[-60:]) / 60.0
                    features["ma60_dist"] = round(((current_c - ma60) / ma60) * 100, 2)
                break
        except:
            pass

    # 步骤 3：雅虎补量
    if not vols:
        try:
            suffix = ".SS" if proxy_code.startswith("5") else ".SZ"
            df_yf = fetch_with_timeout(get_yf_history, 4, f"{proxy_code}{suffix}", "6d")
            if len(df_yf) >= 2:
                vols = df_yf['Volume'].tolist()
        except:
            pass

    if vols and len(vols) >= 2:
        ratios = [round(vols[i]/vols[i-1], 2) if vols[i-1]>0 else 1.0 for i in range(1, len(vols))]
        last_ratio = ratios[-1]
        if last_ratio > 1.2: features["tactical_desc"] = f"放量({last_ratio})"
        elif last_ratio < 0.8: features["tactical_desc"] = f"缩量({last_ratio})"
        else: features["tactical_desc"] = f"平量({last_ratio})"

    return features

# ==========================================
# 3. 组装情报 (向 AI 喂入均线状态)
# ==========================================
def collect_v3_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tac_macro, strat_macro = get_macro_v3()
    
    dash_data = sh.worksheet("Dashboard").get_all_values()
    headers = dash_data[0]
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    strategic_archive = {"macro_matrix": strat_macro, "active_positions": []}
    tactical_etfs, tactical_rules, exec_template = [], [], []

    macro_str = " | ".join([f"{k}:{v}" for k, v in tac_macro.items()])

    for row in dash_data[1:]:
        if not row or not str(row[0]).strip().isdigit(): continue
        name = row[get_idx("基金名称")]
        proxy = re.search(r'\d{6}', row[get_idx("替身代码")] if get_idx("替身代码") != -1 else "").group(0) if re.search(r'\d{6}', row[get_idx("替身代码")] if get_idx("替身代码") != -1 else "") else ""
        rule = row[get_idx("战术纪律")] if get_idx("战术纪律") != -1 else ""
        
        try:
            s_str = row[get_idx("持有份额")] if get_idx("持有份额") != -1 else ""
            shares = float(re.sub(r'[^\d.]', '', s_str)) if re.sub(r'[^\d.]', '', s_str) else 0.0
            
            n_str = row[get_idx("最新净值")] if get_idx("最新净值") != -1 else ""
            nav = float(re.sub(r'[^\d.]', '', n_str)) if re.sub(r'[^\d.]', '', n_str) else 0.0
            if nav == 0.0:
                p_str = row[get_idx("前一日净值")] if get_idx("前一日净值") != -1 else ""
                nav = float(re.sub(r'[^\d.]', '', p_str)) if re.sub(r'[^\d.]', '', p_str) else 0.0
            if nav == 0.0:
                c_str = row[get_idx("持仓成本")] if get_idx("持仓成本") != -1 else ""
                nav = float(re.sub(r'[^\d.]', '', c_str)) if re.sub(r'[^\d.]', '', c_str) else 0.0
                
            val = shares * nav
            pos_str = f"¥{val/1000:.1f}k" if val > 0 else "¥0"
        except:
            pos_str = "未知"

        if rule: tactical_rules.append(f"- **{name}**：{rule}")
        # V4.0 执行单模板
        exec_template.append(f"- **{name}**：[指令] | ￥[金额] | [极简理由。若触发纪律则必须标红🚨]")
        
        if "纳斯达克" in name or "标普" in name:
            ixic_pct = strat_macro.get("^IXIC", {}).get("daily_pct", 0.0)
            nq_pct = strat_macro.get("NQ=F", {}).get("daily_pct", 0.0)
            tactical_etfs.append(f"* **{name}**: 昨夜 {ixic_pct:+.2f}% | 期指 {nq_pct:+.2f}% | 仓:{pos_str}")
            strategic_archive["active_positions"].append({"name": name, "today_pct": ixic_pct, "is_qdii": True})
        else:
            if proxy:
                features = get_realtime_etf_features(proxy)
                pct = features["today_pct"]
                # 🎯 V4.0：直接把均线乖离率打印在情报里喂给 AI 和你
                ma_info = f"MA20乖离:{features['ma20_dist']:+.2f}%"
                tactical_etfs.append(f"* **{name}**({proxy}): {pct:+.2f}% | 仓:{pos_str} | {features['tactical_desc']} | {ma_info}")
                strategic_archive["active_positions"].append({"name": name, "proxy": proxy, "today_pct": pct})

    # 雷达池部分保持精简
    tactical_radar = []
    try:
        radar_data = sh.worksheet("雷达监控").get_all_values()
        r_headers = radar_data[0]
        def r_idx(kw): return next((i for i, h in enumerate(r_headers) if kw in h), -1)
        for row in radar_data[1:]:
            if not row or not any(row): continue
            r_name = row[r_idx("板块名称")] if r_idx("板块名称") != -1 else ""
            r_proxy_raw = row[r_idx("替身代码")] if r_idx("替身代码") != -1 else ""
            r_proxy = re.search(r'\d{6}', r_proxy_raw).group(0) if re.search(r'\d{6}', r_proxy_raw) else ""
            r_trigger = row[r_idx("狙击触发条件")] if r_idx("狙击触发条件") != -1 else ""
            if r_proxy:
                features = get_realtime_etf_features(r_proxy)
                ma_info = f"MA60乖离:{features['ma60_dist']:+.2f}%"
                tactical_radar.append(f"* **{r_name}**: {features['today_pct']:+.2f}% | {ma_info} | 🎯扳机: {r_trigger}")
    except: pass

    etfs_str = "\n".join(tactical_etfs) if tactical_etfs else "暂无数据。"
    radar_str = "\n".join(tactical_radar) if tactical_radar else "无配置标的。"
    
    md_prompt = f"""## 🌍 宏观水位
{macro_str}

## 🎯 场内盘口与绝对均线状态
{etfs_str}

## 📡 雷达池 (4万备用金)
{radar_str}
"""
    return md_prompt, "\n".join(tactical_rules), "\n".join(exec_template), strategic_archive

# ==========================================
# 4. AI 顶级大脑 (V4.0 冷血判官模式)
# ==========================================
def ask_v3_tactical_agent(md_prompt: str, rules_str: str, exec_str: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V4.0 顶级量化决策系统 (The Executioner)
你不再是一个只会给建议的参谋，你现在是手握生杀大权的冷血判官！
在14:45，人类指挥官极易受主观情绪干扰。你的任务是：读取传入的绝对客观数据（尤其是 MA20/MA60乖离率），严格对照【战术纪律】，下达冰冷的判决书！

【输入情报】：
{md_prompt}
【战术纪律】：
{rules_str}

# V4.0 绝对指令（必须遵守）：
1. 【宏观诊断】与【阵地推演】：依然要求你保持极高含金量的深度分析，深度剖析资金高低切换、KOSPI映射与科技巨头资本开支流向。
2. 【执行单裁决】：
   - 你必须读取数据中的 `MA20乖离`。如果乖离率是负数（如 -1.50%），说明已经跌破20日线！
   - 如果纪律里写了“破20日线”，且数据确实跌破了，**你必须在执行单前方加上 🚨，并下达绝对清仓/减仓指令，标明 [绝对证伪触发]**！
   - 如果未破线，指令必须是极其笃定的“锁仓死拿，逻辑成立”。
   - 绝不允许让用户自己去“看盘确认”！你就是最终裁判！

# Output Format：
### 🌍 [宏观主线诊断]
(深度剖析资金流向与流动性，约 150 字)

### 🧠 [阵地推演]
(深度推演今日战术重心，约 150 字)

### 📝 [15:00 申赎执行单]
{exec_str}
    """
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(temperature=0.1) # 极度冷血的温度
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
                "content": f"🚀 **V4.0 全自动判决系统 (冷血模式)**\n\n{full_content}"
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
