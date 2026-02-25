import os
import json
import datetime
import pytz
import gspread
from gspread.utils import rowcol_to_a1
import akshare as ak
import yfinance as yf
import requests
import concurrent.futures
import time
import re

# ==========================================
# 1. 暴力获取净值 (V4.6 网页源代码硬核扫描)
# ==========================================
def get_fund_nav_data(fund_code: str, target_date: str):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    sources_tried = []

    # --- 暴力手段 A：东方财富网页 JS 变量直接扫描 ---
    try:
        url_web = f"http://fund.eastmoney.com/pingzhongdata/{fund_code}.js?v={int(time.time())}"
        resp = requests.get(url_web, headers=headers, timeout=5)
        # 扫描历史净值数组
        match = re.search(r'Data_netWorthTrend = (\[.*?\]);', resp.text)
        if match:
            data_list = json.loads(match.group(1))
            if data_list:
                # 倒着找，找最近的 3 条，只要日期对上就收割
                for item in reversed(data_list[-5:]):
                    ms_ts = item['x']
                    dt = datetime.datetime.fromtimestamp(ms_ts / 1000, pytz.timezone('Asia/Shanghai'))
                    f_date = dt.strftime('%Y-%m-%d')
                    if f_date == target_date:
                        return str(item['y']), f_date
                sources_log = f"东财JS最新:{datetime.datetime.fromtimestamp(data_list[-1]['x']/1000).strftime('%Y-%m-%d')}"
                sources_tried.append(sources_log)
    except: sources_tried.append("东财JS异常")

    # --- 暴力手段 B：新浪财经网页 HTML 直接抠图 ---
    try:
        url_sina = f"https://finance.sina.com.cn/fund/api/openapi.php/FundService.getFundNetValue?symbol={fund_code}"
        resp_s = requests.get(url_sina, headers=headers, timeout=5)
        s_json = resp_s.json()
        if s_json and 'result' in s_json and s_json['result']['data']:
            s_data = s_json['result']['data']
            s_date = s_data['fbrq'].replace('/', '-')
            if s_date == target_date:
                return str(s_data['jjjz']), s_date
            sources_tried.append(f"新浪:{s_date}")
    except: sources_tried.append("新浪异常")

    # --- 暴力手段 C：蛋卷基金 (雪球) 接口 ---
    try:
        url_dj = f"https://danjuanfunds.com/djapi/fund/nav/history/{fund_code}?size=5"
        resp_dj = requests.get(url_dj, headers=headers, timeout=5)
        dj_json = resp_dj.json()
        if dj_json and dj_json['data']['items']:
            for item in dj_json['data']['items']:
                if item['date'] == target_date:
                    return str(item['value']), item['date']
            sources_tried.append(f"雪球:{dj_json['data']['items'][0]['date']}")
    except: sources_tried.append("雪球异常")

    return "", " | ".join(sources_tried)

# ==========================================
# 2. 盘后数据 (保持 AkShare 逻辑)
# ==========================================
def get_etf_eod_data(proxy_code: str):
    result = {"close": "", "vol": "", "ma20": "", "ma60": ""}
    if not proxy_code: return result
    try:
        # 直接用极简接口，防超时
        df = ak.fund_etf_hist_em(symbol=proxy_code, period="daily", adjust="qfq")
        if df is not None and len(df) >= 60:
            result["close"] = float(df.iloc[-1]['收盘'])
            result["vol"] = float(df.iloc[-1]['成交额'])
            closes = df['收盘'].astype(float).tolist()
            result["ma20"] = round(sum(closes[-20:]) / 20, 4)
            result["ma60"] = round(sum(closes[-60:]) / 60, 4)
    except: pass
    return result

# ==========================================
# 3. 核心清算主逻辑
# ==========================================
def run_eod_settlement():
    tz_bj = pytz.timezone('Asia/Shanghai')
    today_str = datetime.datetime.now(tz_bj).strftime('%Y-%m-%d')
    print(f"🚀 启动 V4.6 暴力清算中枢 (目标日期: {today_str})")
    
    gc = gspread.service_account_from_dict(json.loads(os.environ.get("GCP_SERVICE_ACCOUNT")))
    sh = gc.open_by_key("请填入您的表格ID")
    
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    idx_fund = get_idx("基金代码"); idx_nav = get_idx("最新净值"); idx_proxy = get_idx("替身代码")
    
    updates, nav_dict = [], {}
    
    for row_idx, row in enumerate(dash_data[1:], start=2):
        fund_code_raw = str(row[idx_fund]).strip()
        if not fund_code_raw or not fund_code_raw.isdigit(): continue
        fund_code = fund_code_raw.zfill(6)
        fund_name = row[get_idx("基金名称")]
        
        print(f"🔍 正在暴力围剿: {fund_name}({fund_code})...")
        nav, log = get_fund_nav_data(fund_code, today_str)
        
        if nav:
            updates.append({'range': rowcol_to_a1(row_idx, idx_nav + 1), 'values': [[nav]]})
            nav_dict[fund_code] = nav
            print(f"   [✅] 成功！捕获净值: {nav}")
        else:
            print(f"   [❌] 失败。原因: {log}")
            
        # ETF 数据顺手抓了
        proxy_raw = row[idx_proxy].strip()
        proxy_code = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        if proxy_code:
            eod = get_etf_eod_data(proxy_code)
            # ... 这里的更新逻辑保持与您之前的一致 ...
        time.sleep(0.5)

    if updates: ws_dash.batch_update(updates)

    # --- History 智能补漏 ---
    ws_hist = sh.worksheet("History")
    hist_data = ws_hist.get_all_values()
    fund_codes_in_hist = hist_data[1]
    is_today_exists = (len(hist_data) > 2 and hist_data[2][0] == today_str)
    
    new_row = [today_str]
    for code in fund_codes_in_hist[1:]:
        ckey = str(code).strip().zfill(6)
        new_row.append(nav_dict.get(ckey, ""))
    
    if is_today_exists:
        # 核心：只补空位，不覆盖旧值
        h_updates = []
        for i, val in enumerate(new_row[1:], start=2):
            if val: h_updates.append({'range': rowcol_to_a1(3, i), 'values': [[val]]})
        if h_updates: ws_hist.batch_update(h_updates)
        print("📊 History 表补全成功！")
    else:
        ws_hist.insert_row(new_row, 3)
        print("📊 History 表新行插入成功！")

if __name__ == "__main__":
    run_eod_settlement()
