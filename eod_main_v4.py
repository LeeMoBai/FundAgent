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
# 1. 获取场外基金净值 (V4.5 三源狙击：天天+雪球+东财网页)
# ==========================================
def get_fund_nav_data(fund_code: str, target_date: str):
    ts = int(time.time() * 1000)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    sources_log = []

    # --- 渠道一：蛋卷基金 (雪球/DanJuan) - 响应速度通常极快 ---
    try:
        url_dj = f"https://danjuanfunds.com/djapi/fund/nav/history/{fund_code}?size=10"
        resp_dj = requests.get(url_dj, headers=headers, timeout=5)
        dj_data = resp_dj.json()
        if dj_data and 'data' in dj_data and dj_data['data']['items']:
            latest_item = dj_data['data']['items'][0]
            # 蛋卷返回格式通常是 "2026-02-25"
            f_date = latest_item['date']
            if f_date == target_date:
                return str(latest_item['value']), f_date
            sources_log.append(f"雪球:{f_date}")
    except: sources_log.append("雪球:异常")

    # --- 渠道二：天天基金 (移动端 API) ---
    try:
        url_tt = f"https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList?FCODE={fund_code}&pageIndex=1&pageSize=5&_={ts}"
        resp_tt = requests.get(url_tt, headers=headers, timeout=5)
        tt_data = resp_tt.json()
        if tt_data and 'Datas' in tt_data and tt_data['Datas']:
            for item in tt_data['Datas']:
                f_date = item['FSRQ'].replace('/', '-')
                if f_date == target_date:
                    return str(item['NAV']), f_date
            sources_log.append(f"天天:{tt_data['Datas'][0]['FSRQ'].replace('/', '-')}")
    except: sources_log.append("天天:异常")

    # --- 渠道三：东方财富 (网页版底层 JS 接口) ---
    try:
        url_web = f"http://fund.eastmoney.com/pingzhongdata/{fund_code}.js?v={datetime.datetime.now().strftime('%Y%m%d%H%M')}"
        resp_web = requests.get(url_web, headers=headers, timeout=5)
        # 网页版数据在 JS 变量 Data_netWorthTrend 中
        match = re.search(r'Data_netWorthTrend = (\[.*?\]);', resp_web.text)
        if match:
            net_worth_data = json.loads(match.group(1))
            if net_worth_data:
                # 网页版通常是毫秒时间戳 [timestamp, nav, percentage, ...]
                latest_item = net_worth_data[-1]
                dt_object = datetime.datetime.fromtimestamp(latest_item['x'] / 1000, pytz.timezone('Asia/Shanghai'))
                f_date = dt_object.strftime('%Y-%m-%d')
                if f_date == target_date:
                    return str(latest_item['y']), f_date
                sources_log.append(f"东财网页:{f_date}")
    except: sources_log.append("网页:异常")

    return "", " | ".join(sources_log)

# ==========================================
# 2. 获取场内ETF盘后真实数据 (保持不变)
# ==========================================
def get_etf_eod_data(proxy_code: str):
    result = {"close": "", "vol": "", "ma20": "", "ma60": "", "ma250": ""}
    if not proxy_code: return result
    try:
        hist_df = fetch_with_timeout(ak.fund_etf_hist_em, 8, symbol=proxy_code, period="daily", adjust="qfq")
        if hist_df is not None and len(hist_df) >= 2:
            closes = [float(x) for x in hist_df['收盘'].tolist()]
            result["close"] = closes[-1]
            result["vol"] = float(hist_df.iloc[-1]['成交额'])
            if len(closes) >= 20: result["ma20"] = round(sum(closes[-20:]) / 20.0, 4)
            if len(closes) >= 60: result["ma60"] = round(sum(closes[-60:]) / 60.0, 4)
            return result
    except: pass
    return result

# ==========================================
# 3. 执行静默写入任务 (V4.5 智能对账版)
# ==========================================
def run_eod_settlement():
    tz_bj = pytz.timezone('Asia/Shanghai')
    today_str = datetime.datetime.now(tz_bj).strftime('%Y-%m-%d')
    print(f"🌙 启动 V4.5 EOD 结算系统 (目标日期: {today_str})...")
    
    gc = get_gspread_client()
    sh = gc.open_by_key("1kKz9snuCeMSKwBCBGRBBUo8P-04C72Dx5Pt3ArYvtRw") 
    
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    idx_fund = get_idx("基金代码"); idx_proxy = get_idx("替身代码"); idx_nav = get_idx("最新净值")
    idx_close = get_idx("[EOD]昨收盘价"); idx_vol = get_idx("[EOD]昨成交额")
    idx_ma20 = get_idx("[EOD]MA20"); idx_ma60 = get_idx("[EOD]MA60")

    updates, eod_json_state, nav_dict = [], {}, {} 
    
    for row_idx, row in enumerate(dash_data[1:], start=2):
        fund_code_raw = str(row[idx_fund]).strip() if idx_fund != -1 else ""
        if not fund_code_raw or not any(row): continue
        fund_code = fund_code_raw.zfill(6)
        fund_name = row[get_idx("基金名称")]
        proxy_raw = row[idx_proxy].strip() if idx_proxy != -1 else ""
        proxy_code = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        
        # 三源抓取
        nav, found_log = get_fund_nav_data(fund_code, today_str)
        
        if nav:
            if idx_nav != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_nav + 1), 'values': [[nav]]})
            nav_dict[fund_code] = nav
            print(f"   [√] {fund_name}: 成功捕获 {today_str} 净值 {nav}")
        else:
            print(f"   [!] {fund_name}: 数据源均未同步 ({found_log})")
        
        if proxy_code:
            eod = get_etf_eod_data(proxy_code)
            if eod["close"] != "": updates.append({'range': rowcol_to_a1(row_idx, idx_close + 1), 'values': [[eod["close"]]]})
            if eod["vol"] != "": updates.append({'range': rowcol_to_a1(row_idx, idx_vol + 1), 'values': [[eod["vol"]]]})
            if eod["ma20"] != "": updates.append({'range': rowcol_to_a1(row_idx, idx_ma20 + 1), 'values': [[eod["ma20"]]]})
            if eod["ma60"] != "": updates.append({'range': rowcol_to_a1(row_idx, idx_ma60 + 1), 'values': [[eod["ma60"]]]})
        
        eod_json_state[fund_name] = {"nav": nav, "last_source": found_log}
        time.sleep(0.3)

    if updates: ws_dash.batch_update(updates)
    
    # --- History 智能填缝 ---
    try:
        ws_hist = sh.worksheet("History")
        hist_data = ws_hist.get_all_values()
        if len(hist_data) >= 2:
            fund_codes_in_hist = hist_data[1]
            is_today_row_exists = (len(hist_data) > 2 and hist_data[2][0] == today_str)
            
            new_row = [today_str]
            for code in fund_codes_in_hist[1:]:
                code_key = str(code).strip().zfill(6)
                new_row.append(nav_dict.get(code_key, ""))
            
            if is_today_row_exists:
                final_updates = []
                for i in range(1, len(new_row)):
                    if new_row[i] != "": final_updates.append({'range': rowcol_to_a1(3, i+1), 'values': [[new_row[i]]]})
                if final_updates: ws_hist.batch_update(final_updates)
                print("   [√] History 补漏完成。")
            else:
                ws_hist.insert_row(new_row, 3) 
                print("   [√] History 新行插入完成。")
    except Exception as e: print(f"   [!] History 更新异常: {e}")

    print("✅ EOD 结算全部完成！")

if __name__ == "__main__":
    run_eod_settlement()
