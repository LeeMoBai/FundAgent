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

# ==========================================
# 0. 基础设置与全能防弹衣
# ==========================================
def fetch_with_timeout(func, timeout_sec, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_sec)
        except Exception:
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
# 2. 获取场内ETF盘后真实数据
# ==========================================
def get_etf_eod_data(proxy_code: str):
    result = {"close": "", "vol": "", "ma20": "", "ma60": "", "ma250": ""}
    if not proxy_code: return result

    hist_df = fetch_with_timeout(ak.fund_etf_hist_em, 6, symbol=proxy_code, period="daily", adjust="qfq")
    if hist_df is not None and len(hist_df) >= 2:
        closes = [float(x) for x in hist_df['收盘'].tolist()]
        result["close"] = closes[-1]
        result["vol"] = float(hist_df.iloc[-1]['成交额'])
        if len(closes) >= 20: result["ma20"] = round(sum(closes[-20:]) / 20.0, 4)
        if len(closes) >= 60: result["ma60"] = round(sum(closes[-60:]) / 60.0, 4)
        if len(closes) >= 250: result["ma250"] = round(sum(closes[-250:]) / 250.0, 4)
        return result

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
# 3. 执行静默写入与历史归档任务
# ==========================================
def run_eod_settlement():
    print("🌙 启动 V4.1 EOD 静默后勤清算系统...")
    gc = get_gspread_client()
    sh = gc.open_by_key("请填入您的表格ID") # <--- 请在这里填入您的物理乱码 ID
    
    # --- 任务 A: 刷新 Dashboard ---
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    idx_fund = headers.index("基金代码") if "基金代码" in headers else -1
    idx_proxy = headers.index("替身代码 (ETF)") if "替身代码 (ETF)" in headers else -1
    idx_nav = headers.index("最新净值") if "最新净值" in headers else -1
    idx_close = headers.index("[EOD]昨收盘价") if "[EOD]昨收盘价" in headers else -1
    idx_vol = headers.index("[EOD]昨成交额") if "[EOD]昨成交额" in headers else -1
    idx_ma20 = headers.index("[EOD]MA20点位") if "[EOD]MA20点位" in headers else -1

    updates = []
    eod_json_state = {} # 用于存储绝对准确的盘后数据给AI复盘
    nav_dict = {} # 用于存入 History 表
    
    for row_idx, row in enumerate(dash_data[1:], start=2):
        if not row or (idx_fund != -1 and not row[idx_fund].strip().isdigit()): continue
            
        fund_code = row[idx_fund].strip() if idx_fund != -1 else ""
        fund_name = row[headers.index("基金名称")]
        proxy_raw = row[idx_proxy].strip() if idx_proxy != -1 else ""
        import re
        proxy_code = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        
        fund_state = {}
        
        # 1. 抓取真实净值
        if fund_code and idx_nav != -1:
            nav = get_fund_nav(fund_code)
            if nav != "":
                updates.append({'range': rowcol_to_a1(row_idx, idx_nav + 1), 'values': [[nav]]})
                fund_state["final_nav"] = nav
                nav_dict[fund_code] = nav
        
        # 2. 抓取 ETF 盘后数据
        if proxy_code:
            eod = get_etf_eod_data(proxy_code)
            if eod["close"] != "" and idx_close != -1:
                updates.append({'range': rowcol_to_a1(row_idx, idx_close + 1), 'values': [[eod["close"]]]})
                fund_state["etf_close"] = eod["close"]
            if eod["vol"] != "" and idx_vol != -1:
                updates.append({'range': rowcol_to_a1(row_idx, idx_vol + 1), 'values': [[eod["vol"]]]})
            if eod["ma20"] != "" and idx_ma20 != -1:
                updates.append({'range': rowcol_to_a1(row_idx, idx_ma20 + 1), 'values': [[eod["ma20"]]]})
        
        eod_json_state[fund_name] = fund_state
        time.sleep(0.5)

    if updates:
        ws_dash.batch_update(updates)
        print(f"   [√] Dashboard 核心阵地已填满明天的弹药！")

    # --- 任务 B: 自动追加 History 表 ---
    try:
        ws_hist = sh.worksheet("History")
        hist_data = ws_hist.get_all_values()
        if len(hist_data) >= 2:
            fund_codes_in_hist = hist_data[1] # 第二行是基金代码
            new_row = [datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d')]
            for code in fund_codes_in_hist[1:]: # 跳过第一列的"日期"
                new_row.append(nav_dict.get(code.strip(), ""))
            
            ws_hist.insert_row(new_row, 3) # 插在第3行（最新的数据在最上面）
            print("   [√] 历史净值曲线表 (History) 追加成功！")
    except Exception as e:
        print(f"   [!] History 表更新异常: {e}")

    # --- 任务 C: 生成供 AI 复盘用的绝对结算 JSON ---
    archive_json = {
        "timestamp": datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        "type": "EOD_MARKET_STATE",
        "description": "这是盘后真实结算数据，供周末/月末宏观复盘使用",
        "portfolio_eod": eod_json_state
    }
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/EOD_State_{datetime.datetime.now().strftime('%Y%m%d')}.json", "w", encoding="utf-8") as f:
        json.dump(archive_json, f, ensure_ascii=False, indent=2)

    print("✅ EOD 结算全部完成！")

if __name__ == "__main__":
    run_eod_settlement()
