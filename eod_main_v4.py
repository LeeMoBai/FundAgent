import os
import json
import datetime
import pytz
import gspread
from gspread.utils import rowcol_to_a1
import requests
import time
import re

# ==========================================
# 0. 基础设置与 API 鉴权
# ==========================================
def get_gspread_client():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("缺失 GCP_SERVICE_ACCOUNT")
    return gspread.service_account_from_dict(json.loads(creds_json))

# ==========================================
# 1. 暴力获取净值 (网页源代码硬核扫描)
# ==========================================
def get_fund_nav_data(fund_code: str, target_date: str):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    sources_tried = []

    # A: 东财 JS 暴力扫描
    try:
        url_web = f"http://fund.eastmoney.com/pingzhongdata/{fund_code}.js?v={int(time.time())}"
        resp = requests.get(url_web, headers=headers, timeout=5)
        match = re.search(r'Data_netWorthTrend = (\[.*?\]);', resp.text)
        if match:
            data_list = json.loads(match.group(1))
            if data_list:
                for item in reversed(data_list[-5:]):
                    f_date = datetime.datetime.fromtimestamp(item['x'] / 1000, pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d')
                    if f_date == target_date: return str(item['y']), f_date
                sources_tried.append(f"东财JS:{datetime.datetime.fromtimestamp(data_list[-1]['x']/1000).strftime('%Y-%m-%d')}")
    except: sources_tried.append("东财异常")

    # B: 新浪 HTML 抠图
    try:
        url_sina = f"https://finance.sina.com.cn/fund/api/openapi.php/FundService.getFundNetValue?symbol={fund_code}"
        s_json = requests.get(url_sina, headers=headers, timeout=5).json()
        if s_json and 'result' in s_json and s_json['result']['data']:
            s_date = s_json['result']['data']['fbrq'].replace('/', '-')
            if s_date == target_date: return str(s_json['result']['data']['jjjz']), s_date
            sources_tried.append(f"新浪:{s_date}")
    except: sources_tried.append("新浪异常")

    # C: 雪球 API
    try:
        url_dj = f"https://danjuanfunds.com/djapi/fund/nav/history/{fund_code}?size=5"
        dj_json = requests.get(url_dj, headers=headers, timeout=5).json()
        if dj_json and 'data' in dj_json and dj_json['data']['items']:
            for item in dj_json['data']['items']:
                if item['date'] == target_date: return str(item['value']), item['date']
            sources_tried.append(f"雪球:{dj_json['data']['items'][0]['date']}")
    except: sources_tried.append("雪球异常")

    return "", " | ".join(sources_tried)

# ==========================================
# 2. 暴力抓取 ETF 盘后数据 (极简新浪 K线直连版)
# ==========================================
def get_etf_eod_data(proxy_code: str):
    result = {"close": "", "vol": "", "ma20": "", "ma60": ""}
    if not proxy_code: return result
    
    prefix = "sh" if proxy_code.startswith("5") else "sz"
    symbol = prefix + proxy_code
    
    try:
        # 🛡️ 绝杀技：直接去新浪底层拉取过去 65 天的 K 线 JSON 数组，自己算均线！
        url_kline = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen=65"
        resp = requests.get(url_kline, timeout=5)
        kline_data = json.loads(resp.text)
        
        if kline_data and len(kline_data) > 0:
            closes = [float(day['close']) for day in kline_data]
            if len(closes) >= 20: result["ma20"] = round(sum(closes[-20:]) / 20.0, 4)
            if len(closes) >= 60: result["ma60"] = round(sum(closes[-60:]) / 60.0, 4)
            
        # 再去腾讯现价接口抢一个最精确的收盘价和成交额（万元转元）兜底
        url_qt = f"http://qt.gtimg.cn/q={symbol}"
        qt_resp = requests.get(url_qt, timeout=3)
        qt_data = qt_resp.text.split('~')
        if len(qt_data) > 40:
            result["close"] = float(qt_data[3])
            result["vol"] = float(qt_data[37]) * 10000 
            
        print(f"   [ETF-OK] {proxy_code} 暴力计算完毕: MA20={result['ma20']}, MA60={result['ma60']}")
    except Exception as e:
        print(f"   [ETF-Error] {proxy_code} 抓取失败: {e}")
        
    return result

# ==========================================
# 3. 核心清算主逻辑
# ==========================================
def run_eod_settlement():
    tz_bj = pytz.timezone('Asia/Shanghai')
    now_bj = datetime.datetime.now(tz_bj)
    
    # 智能时差偏移 (18:00 前跑算昨天的补漏)
    if now_bj.hour < 18:
        target_date_obj = now_bj - datetime.timedelta(days=1)
        while target_date_obj.weekday() > 4: target_date_obj -= datetime.timedelta(days=1)
        today_str = target_date_obj.strftime('%Y-%m-%d')
    else:
        today_str = now_bj.strftime('%Y-%m-%d')
        
    print(f"🚀 启动 V4.9 极简暴力清算 (结算目标: {today_str})")
    
    gc = get_gspread_client()
    sh = gc.open_by_key("1kKz9snuCeMSKwBCBGRBBUo8P-04C72Dx5Pt3ArYvtRw") # <--- 【总司令注意：填入您的 ID】
    
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    idx_fund = get_idx("基金代码"); idx_nav = get_idx("最新净值"); idx_proxy = get_idx("替身代码")
    idx_close = get_idx("[EOD]昨收盘价"); idx_vol = get_idx("[EOD]昨成交额")
    idx_ma20 = get_idx("[EOD]MA20"); idx_ma60 = get_idx("[EOD]MA60")
    
    updates, nav_dict, eod_json_state = [], {}, {}
    
    for row_idx, row in enumerate(dash_data[1:], start=2):
        fund_code_raw = str(row[idx_fund]).strip() if idx_fund != -1 else ""
        if not fund_code_raw or not fund_code_raw.isdigit(): continue
        fund_code = fund_code_raw.zfill(6)
        fund_name = row[get_idx("基金名称")]
        
        print(f"🔍 正在暴力围剿: {fund_name}({fund_code})...")
        nav, log = get_fund_nav_data(fund_code, today_str)
        
        fund_state = {"nav": nav, "last_source_log": log}
        if nav:
            if idx_nav != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_nav + 1), 'values': [[nav]]})
            nav_dict[fund_code] = nav
            
        proxy_raw = row[idx_proxy].strip() if idx_proxy != -1 else ""
        proxy_code = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        if proxy_code:
            eod = get_etf_eod_data(proxy_code)
            fund_state.update(eod)
            if eod["close"] != "" and idx_close != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_close + 1), 'values': [[eod["close"]]]})
            if eod["vol"] != "" and idx_vol != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_vol + 1), 'values': [[eod["vol"]]]})
            if eod["ma20"] != "" and idx_ma20 != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_ma20 + 1), 'values': [[eod["ma20"]]]})
            if eod["ma60"] != "" and idx_ma60 != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_ma60 + 1), 'values': [[eod["ma60"]]]})
        
        eod_json_state[fund_name] = fund_state
        time.sleep(0.3)

    if updates: ws_dash.batch_update(updates)

    # --- 🎯 强力刷新雷达监控表 ---
    try:
        ws_radar = sh.worksheet("雷达监控")
        radar_data = ws_radar.get_all_values()
        r_headers = radar_data[0]
        r_idx_proxy = next((i for i, h in enumerate(r_headers) if "替身代码" in h), -1)
        r_idx_ma20 = next((i for i, h in enumerate(r_headers) if "[EOD]MA20" in h), -1)
        r_idx_ma60 = next((i for i, h in enumerate(r_headers) if "[EOD]MA60" in h), -1)
        
        r_updates = []
        for row_idx, row in enumerate(radar_data[1:], start=2):
            if not row or not any(row): continue
            r_proxy_raw = row[r_idx_proxy].strip() if r_idx_proxy != -1 else ""
            r_proxy_code = re.search(r'\d{6}', r_proxy_raw).group(0) if re.search(r'\d{6}', r_proxy_raw) else ""
            if r_proxy_code:
                eod = get_etf_eod_data(r_proxy_code)
                if eod["ma20"] != "" and r_idx_ma20 != -1: r_updates.append({'range': rowcol_to_a1(row_idx, r_idx_ma20 + 1), 'values': [[eod["ma20"]]]})
                if eod["ma60"] != "" and r_idx_ma60 != -1: r_updates.append({'range': rowcol_to_a1(row_idx, r_idx_ma60 + 1), 'values': [[eod["ma60"]]]})
        if r_updates: 
            ws_radar.batch_update(r_updates)
            print("📡 雷达监控表更新完毕！")
    except Exception as e: print(f"   [!] 雷达池更新异常: {e}")

    # --- History 智能补漏 ---
    try:
        ws_hist = sh.worksheet("History")
        hist_data = ws_hist.get_all_values()
        fund_codes_in_hist = hist_data[1]
        is_target_row_exists = (len(hist_data) > 2 and hist_data[2][0] == today_str)
        
        new_row = [today_str]
        for code in fund_codes_in_hist[1:]:
            ckey = str(code).strip().zfill(6)
            new_row.append(nav_dict.get(ckey, ""))
        
        if is_target_row_exists:
            h_updates = []
            for i, val in enumerate(new_row[1:], start=2):
                if val: h_updates.append({'range': rowcol_to_a1(3, i), 'values': [[val]]})
            if h_updates: ws_hist.batch_update(h_updates)
            print(f"📊 History 表 {today_str} 已存在，空位补缺完成！")
        else:
            ws_hist.insert_row(new_row, 3)
            print(f"📊 History 表新行 {today_str} 插入成功！")
    except Exception as e:
        print(f"   [!] History 补漏异常: {e}")

    # 归档 JSON
    archive_json = {
        "timestamp": datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        "target_date": today_str,
        "portfolio_eod": eod_json_state
    }
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/EOD_State_{today_str.replace('-','')}.json", "w", encoding="utf-8") as f:
        json.dump(archive_json, f, ensure_ascii=False, indent=2)
    print("✅ EOD 极简暴力清算全部完成！")

if __name__ == "__main__":
    run_eod_settlement()
