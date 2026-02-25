import os
import json
import gspread
from gspread.utils import rowcol_to_a1
import akshare as ak
import yfinance as yf
import requests
import concurrent.futures
import time

# ==========================================
# 0. 基础设置与战术熔断器
# ==========================================
def fetch_with_timeout(func, timeout_sec, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_sec)
        except concurrent.futures.TimeoutError:
            return None

def get_gspread_client():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not creds_json:
        raise ValueError("缺失 GCP_SERVICE_ACCOUNT")
    return gspread.service_account_from_dict(json.loads(creds_json))

# ==========================================
# 1. 获取场外基金净值 (穿透式API)
# ==========================================
def get_fund_nav(fund_code: str):
    """直接调用天天基金底层API，稳定获取真实最新净值（防网页版爬虫失效）"""
    try:
        url = f"https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList?FCODE={fund_code}&pageIndex=1&pageSize=1"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=5)
        data = resp.json()
        if data and 'Datas' in data and data['Datas']:
            return float(data['Datas'][0]['NAV'])
    except Exception as e:
        print(f"   [!] 获取净值失败 {fund_code}: {e}")
    return ""

# ==========================================
# 2. 获取场内ETF盘后数据 (量价与重型均线)
# ==========================================
def get_etf_eod_data(proxy_code: str):
    """盘后获取收盘价、成交额、及各级别均线"""
    result = {"close": "", "vol": "", "ma20": "", "ma60": "", "ma250": ""}
    if not proxy_code:
        return result

    # 优先使用 AkShare (带前复权)
    hist_df = fetch_with_timeout(ak.fund_etf_hist_em, 6, symbol=proxy_code, period="daily", adjust="qfq")
    if hist_df is not None and len(hist_df) >= 2:
        closes = [float(x) for x in hist_df['收盘'].tolist()]
        result["close"] = closes[-1]
        result["vol"] = float(hist_df.iloc[-1]['成交额'])
        
        if len(closes) >= 20: result["ma20"] = round(sum(closes[-20:]) / 20.0, 4)
        if len(closes) >= 60: result["ma60"] = round(sum(closes[-60:]) / 60.0, 4)
        if len(closes) >= 250: result["ma250"] = round(sum(closes[-250:]) / 250.0, 4)
        return result

    # 如果国内接口维护，雅虎财经备用兜底 (拉取1年数据)
    suffix = ".SS" if proxy_code.startswith("5") else ".SZ"
    df_yf = fetch_with_timeout(yf.Ticker(f"{proxy_code}{suffix}").history, 6, period="1y")
    if df_yf is not None and len(df_yf) >= 2:
        closes = df_yf['Close'].tolist()
        result["close"] = round(closes[-1], 3)
        result["vol"] = float(df_yf['Volume'].iloc[-1])
        
        if len(closes) >= 20: result["ma20"] = round(sum(closes[-20:]) / 20.0, 4)
        if len(closes) >= 60: result["ma60"] = round(sum(closes[-60:]) / 60.0, 4)
        if len(closes) >= 250: result["ma250"] = round(sum(closes[-250:]) / 250.0, 4)
        
    return result

# ==========================================
# 3. 执行静默写入任务
# ==========================================
def run_eod_settlement():
    print("🌙 启动 V4.0 EOD 静默后勤清算系统...")
    gc = get_gspread_client()
    sh = gc.open("基金净值总结")
    
    # ---------------------------
    # 任务 A: 刷新 Dashboard 核心表
    # ---------------------------
    print("   [+] 正在核算 Dashboard 核心持仓...")
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    # 获取目标列索引 (0-based)
    idx_fund = headers.index("基金代码") if "基金代码" in headers else -1
    idx_proxy = headers.index("替身代码 (ETF)") if "替身代码 (ETF)" in headers else -1
    idx_nav = headers.index("最新净值") if "最新净值" in headers else -1
    idx_close = headers.index("[EOD]昨收盘价") if "[EOD]昨收盘价" in headers else -1
    idx_vol = headers.index("[EOD]昨成交额") if "[EOD]昨成交额" in headers else -1
    idx_ma20 = headers.index("[EOD]MA20点位") if "[EOD]MA20点位" in headers else -1

    updates = []
    for row_idx, row in enumerate(dash_data[1:], start=2): # 从第2行开始
        if not row or (idx_fund != -1 and not row[idx_fund].strip().isdigit()): 
            continue
            
        fund_code = row[idx_fund].strip() if idx_fund != -1 else ""
        proxy_raw = row[idx_proxy].strip() if idx_proxy != -1 else ""
        import re
        proxy_code = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        
        # 1. 抓取真实净值
        if fund_code and idx_nav != -1:
            nav = get_fund_nav(fund_code)
            if nav != "":
                updates.append({'range': rowcol_to_a1(row_idx, idx_nav + 1), 'values': [[nav]]})
        
        # 2. 抓取 ETF 盘后数据
        if proxy_code:
            eod = get_etf_eod_data(proxy_code)
            if eod["close"] != "" and idx_close != -1:
                updates.append({'range': rowcol_to_a1(row_idx, idx_close + 1), 'values': [[eod["close"]]]})
            if eod["vol"] != "" and idx_vol != -1:
                updates.append({'range': rowcol_to_a1(row_idx, idx_vol + 1), 'values': [[eod["vol"]]]})
            if eod["ma20"] != "" and idx_ma20 != -1:
                updates.append({'range': rowcol_to_a1(row_idx, idx_ma20 + 1), 'values': [[eod["ma20"]]]})
                
        time.sleep(0.5) # 防止API限流

    if updates:
        ws_dash.batch_update(updates)
        print(f"   [√] Dashboard 更新完成！共推送 {len(updates)} 个精准数据点。")

    # ---------------------------
    # 任务 B: 刷新 雷达监控 表
    # ---------------------------
    print("   [+] 正在核算 雷达监控池 数据...")
    try:
        ws_radar = sh.worksheet("雷达监控")
        radar_data = ws_radar.get_all_values()
        r_headers = radar_data[0]
        
        r_idx_proxy = r_headers.index("替身代码") if "替身代码" in r_headers else -1
        r_idx_ma60 = r_headers.index("[EOD]MA60点位") if "[EOD]MA60点位" in r_headers else -1
        r_idx_ma250 = r_headers.index("[EOD]250日年线") if "[EOD]250日年线" in r_headers else -1

        r_updates = []
        for row_idx, row in enumerate(radar_data[1:], start=2):
            if not row or not any(row): continue
            
            proxy_raw = row[r_idx_proxy].strip() if r_idx_proxy != -1 else ""
            import re
            proxy_code = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
            
            if proxy_code:
                eod = get_etf_eod_data(proxy_code)
                if eod["ma60"] != "" and r_idx_ma60 != -1:
                    r_updates.append({'range': rowcol_to_a1(row_idx, r_idx_ma60 + 1), 'values': [[eod["ma60"]]]})
                if eod["ma250"] != "" and r_idx_ma250 != -1:
                    r_updates.append({'range': rowcol_to_a1(row_idx, r_idx_ma250 + 1), 'values': [[eod["ma250"]]]})
            
            time.sleep(0.5)

        if r_updates:
            ws_radar.batch_update(r_updates)
            print(f"   [√] 雷达池更新完成！共推送 {len(r_updates)} 个精准防守点位。")
    except Exception as e:
        print(f"   [!] 雷达表更新异常: {e}")

    print("✅ EOD 结算全部完成，已为明天的空战填满弹药。")

if __name__ == "__main__":
    run_eod_settlement()
