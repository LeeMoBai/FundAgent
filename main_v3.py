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
# 1. 抓取外围宏观数据 (YFinance)
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
            data = fetch_with_timeout(get_yf_history, 4, symbol, "5d")
            if len(data) >= 2:
                closes = data['Close'].tolist()
                pct_chg = ((closes[-1] - closes[-2]) / closes[-2]) * 100
                if "BTC" in display_name or "XAU" in display_name:
                    tactical_macro[display_name] = f"${closes[-1]:,.0f}({pct_chg:+.2f}%)"
                elif "US10Y" in display_name:
                    tactical_macro[display_name] = f"{closes[-1]:.2f}%({pct_chg:+.2f}%)"
                else:
                    tactical_macro[display_name] = f"{closes[-1]:.0f}({pct_chg:+.2f}%)"
                strategic_macro[symbol] = {"current": closes[-1], "daily_pct": pct_chg}
            else:
                tactical_macro[display_name] = "N/A"
        except:
            tactical_macro[display_name] = "盲区"
            
    return tactical_macro, strategic_macro

# ==========================================
# 2. 抓取 ETF (引入腾讯极速接口，绝对真实)
# ==========================================
def get_realtime_etf_features(proxy_code: str) -> dict:
    features = {"today_pct": 0.0, "tactical_desc": "无量价数据", "5d_vol_ratios": [], "ma250_dist": 0.0}
    
    # 🎯 步骤 1：使用腾讯底层 API 抓取绝对真实的实时涨跌幅 (0延迟，绝不报错)
    try:
        prefix = "sh" if proxy_code.startswith("5") else "sz"
        url = f"http://qt.gtimg.cn/q={prefix}{proxy_code}"
        resp = fetch_with_timeout(requests.get, 3, url)
        data = resp.text.split('~')
        if len(data) > 32:
            features["today_pct"] = round(float(data[32]), 2)
    except:
        pass # 如果腾讯也挂了，涨跌幅显示 0.0

    # 🎯 步骤 2：使用 AkShare 抓取历史数据计算量比与乖离率
    try:
        hist_df = fetch_with_timeout(ak.fund_etf_hist_em, 4, symbol=proxy_code, period="daily")
        if len(hist_df) >= 2:
            vols = hist_df.tail(6)['成交额'].tolist()
            if len(hist_df) >= 20:
                ma250 = hist_df.tail(250)['收盘'].mean()
                features["ma250_dist"] = round(((hist_df.iloc[-1]['收盘'] - ma250) / ma250) * 100, 2)
            
            ratios = [round(vols[i]/vols[i-1], 2) if vols[i-1]>0 else 1.0 for i in range(1, len(vols))]
            features["5d_vol_ratios"] = ratios
            last_ratio = ratios[-1]
            if last_ratio > 1.2: features["tactical_desc"] = f"放量({last_ratio})"
            elif last_ratio < 0.8: features["tactical_desc"] = f"缩量({last_ratio})"
            else: features["tactical_desc"] = f"平量({last_ratio})"
    except:
        pass

    return features

# ==========================================
# 3. 组装极度瘦身的经典快照
# ==========================================
def collect_v3_intelligence(gc) -> tuple:
    sh = gc.open("基金净值总结")
    tac_macro, strat_macro = get_macro_v3()
    
    print("   [+] 启动腾讯+东方财富双引擎穿透式抓取...")
    
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
        # 强制 AI 把废话省在执行单里
        exec_template.append(f"- **{name}**：[指令] | ￥[金额] | [理由(极简15字内)。[证伪]:限8字]")
        
        if proxy:
            features = get_realtime_etf_features(proxy)
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
                features = get_realtime_etf_features(r_proxy)
                tactical_radar.append(f"* **{r_name}**: {features['today_pct']:+.2f}% | 🎯扳机: {r_trigger}")
    except: pass

    etfs_str = "\n".join(tactical_etfs) if tactical_etfs else "暂无数据。"
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
# 4. AI 顶级大脑 (恢复研报级含金量)
# ==========================================
def ask_v3_tactical_agent(md_prompt: str, rules_str: str, exec_str: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    prompt = f"""
# Role: V3.0 顶级场外基金量化大脑 (Midday Agent)
当前14:45，基于数据进行兼具深度与纪律的决断。

【输入情报】：
{md_prompt}
【战术纪律】：
{rules_str}

# Output Format：
【排版红线】：[执行单]的理由与证伪必须严格精简！但你可以把省下来的字数，全部投入到【宏观诊断】和【阵地推演】的深度分析中，恢复你顶级分析师的含金量！

### 🌍 [宏观主线诊断]
(结合宏观数据深度剖析全球流动性预期与A股资金高低切换逻辑，展现深度洞察力，约 150-200 字)

### 🧠 [阵地推演]
(深度推演今日战术重心、防守反击逻辑与机会成本，约 150 字)

### 📝 [15:00 执行单]
{exec_str}
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
        full_content = f"{md_prompt}\n====================\n\n{ai_decision}"
        
        # 释放微信空间，给予推演小作文更大的容错率 (4096 字节极限，我们卡在 3900)
        if len(full_content.encode('utf-8')) > 3900:
            full_content = full_content[:1250] + "\n\n...(字数触及上限，安全截断)"
            
        payload = {
            "msgtype": "markdown", 
            "markdown": {
                "content": f"🚀 **V3.0 盘中决策 (含金量拉满版)**\n\n{full_content}"
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
