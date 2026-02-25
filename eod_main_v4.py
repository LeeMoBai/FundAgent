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
# 1. 获取场外基金净值 (穿透缓存 + 精准日期对齐)
# ==========================================
import time

def get_fund_nav_by_date(fund_code: str, target_date: str):
    try:
        # 加上时间戳，彻底击穿天天基金的 CDN 缓存
        ts = int(time.time() * 1000)
        url = f"https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList?FCODE={fund_code}&pageIndex=1&pageSize=10&_={ts}"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=5)
        data = resp.json()
        if data and 'Datas' in data and data['Datas']:
            for item in data['Datas']:
                # 🛡️ 极其严苛：只要这一天的净值！宁可留空绝不张冠李戴
                if item['FSRQ'] == target_date:
                    return str(item['NAV'])
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
# 3. 执行静默写入与历史归档任务 (智能可重入版)
# ==========================================
def run_eod_settlement():
    tz_bj = pytz.timezone('Asia/Shanghai')
    today_str = datetime.datetime.now(tz_bj).strftime('%Y-%m-%d')
    print(f"🌙 启动 V4.3 EOD 静默后勤清算 (目标日期: {today_str})...")
    
    gc = get_gspread_client()
    sh = gc.open_by_key("请填入您的表格ID") # <--- 别忘了填您的 ID
    
    # --- 任务 A: 刷新 Dashboard ---
    ws_dash = sh.worksheet("Dashboard")
    dash_data = ws_dash.get_all_values()
    headers = dash_data[0]
    
    def get_idx(kw): return next((i for i, h in enumerate(headers) if kw in h), -1)
    
    idx_fund = get_idx("基金代码")
    idx_proxy = get_idx("替身代码")
    idx_nav = get_idx("最新净值")
    idx_close = get_idx("[EOD]昨收盘价")
    idx_vol = get_idx("[EOD]昨成交额")
    idx_ma20 = get_idx("[EOD]MA20")
    idx_ma60 = get_idx("[EOD]MA60")

    updates = []
    eod_json_state = {} 
    nav_dict = {} 
    
    for row_idx, row in enumerate(dash_data[1:], start=2):
        fund_code_raw = str(row[idx_fund]).strip() if idx_fund != -1 else ""
        if not fund_code_raw: continue
        
        fund_code = fund_code_raw.zfill(6)
        if not fund_code.isdigit(): continue
            
        fund_name = row[get_idx("基金名称")]
        proxy_raw = row[idx_proxy].strip() if idx_proxy != -1 else ""
        import re
        proxy_code = re.search(r'\d{6}', proxy_raw).group(0) if re.search(r'\d{6}', proxy_raw) else ""
        
        fund_state = {}
        
        # 1. 抓取真实净值 (严格匹配今天)
        nav = get_fund_nav_by_date(fund_code, today_str)
        
        if nav == "":
            print(f"   [!] {fund_name} {today_str} 的净值尚未公布，跳过更新。")
        else:
            if idx_nav != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_nav + 1), 'values': [[nav]]})
            fund_state["final_nav"] = nav
            nav_dict[fund_code] = nav
        
        # 2. 抓取 ETF 盘后数据
        if proxy_code:
            eod = get_etf_eod_data(proxy_code)
            if eod["close"] != "" and idx_close != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_close + 1), 'values': [[eod["close"]]]})
            if eod["vol"] != "" and idx_vol != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_vol + 1), 'values': [[eod["vol"]]]})
            if eod["ma20"] != "" and idx_ma20 != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_ma20 + 1), 'values': [[eod["ma20"]]]})
            if eod["ma60"] != "" and idx_ma60 != -1: updates.append({'range': rowcol_to_a1(row_idx, idx_ma60 + 1), 'values': [[eod["ma60"]]]})
        
        eod_json_state[fund_name] = fund_state
        time.sleep(0.5)

    if updates:
        ws_dash.batch_update(updates)
        print(f"   [√] Dashboard 核心阵地更新完毕！")

    # --- 任务 B: 刷新 雷达监控 ---
    try:
        ws_radar = sh.worksheet("雷达监控")
        radar_data = ws_radar.get_all_values()
        r_headers = radar_data[0]
        def r_idx(kw): return next((i for i, h in enumerate(r_headers) if kw in h), -1)
        
        r_idx_proxy = r_idx("替身代码")
        r_idx_ma20 = r_idx("[EOD]MA20")
        r_idx_ma60 = r_idx("[EOD]MA60")
        
        r_updates = []
        for row_idx, row in enumerate(radar_data[1:], start=2):
            if not row or not any(row): continue
            r_proxy_raw = row[r_idx_proxy].strip() if r_idx_proxy != -1 else ""
            r_proxy_code = re.search(r'\d{6}', r_proxy_raw).group(0) if re.search(r'\d{6}', r_proxy_raw) else ""
            
            if r_proxy_code:
                eod = get_etf_eod_data(r_proxy_code)
                if eod["ma20"] != "" and r_idx_ma20 != -1: r_updates.append({'range': rowcol_to_a1(row_idx, r_idx_ma20 + 1), 'values': [[eod["ma20"]]]})
                if eod["ma60"] != "" and r_idx_ma60 != -1: r_updates.append({'range': rowcol_to_a1(row_idx, r_idx_ma60 + 1), 'values': [[eod["ma60"]]]})
                time.sleep(0.5)
        
        if r_updates:
            ws_radar.batch_update(r_updates)
            print(f"   [√] 雷达监控 均线数据更新完毕！")
    except Exception as e:
        print(f"   [!] 雷达监控 更新异常: {e}")

    # --- 任务 C: 自动追加 History 表 (🛡️核心修复：智能可重入融合机制) ---
    try:
        ws_hist = sh.worksheet("History")
        hist_data = ws_hist.get_all_values()
        if len(hist_data) >= 2:
            fund_codes_in_hist = hist_data[1]
            
            # 判断第三行是否已经是今天的数据
            is_today_exists = (len(hist_data) > 2 and hist_data[2][0] == today_str)
            
            new_row = [today_str]
            for code in fund_codes_in_hist[1:]:
                code_key = str(code).strip().zfill(6)
                new_row.append(nav_dict.get(code_key, ""))
                
            if is_today_exists:
                # 已经有今天的行了，启动智能填缝！
                old_row = hist_data[2]
                cells_to_update = []
                for i in range(1, len(new_row)):
                    # 如果刚才没抓到新数据，但表格里原本有数据，就继承旧数据，绝不覆盖成空
                    if new_row[i] == "" and i < len(old_row):
                        new_row[i] = old_row[i]
                    
                    # 批量打包需要更新的格子
                    cells_to_update.append({'range': rowcol_to_a1(3, i+1), 'values': [[new_row[i]]]})
                    
                if cells_to_update:
                    ws_hist.batch_update(cells_to_update)
                print("   [√] 历史净值表 (History) 今日行已存在，已执行智能填缝补漏！")
            else:
                # 还没有今天的行，直接全新插入
                ws_hist.insert_row(new_row, 3) 
                print("   [√] 历史净值表 (History) 追加新日期成功！")
    except Exception as e:
        print(f"   [!] History 表更新异常: {e}")

    # 生成绝对结算 JSON
    archive_json = {
        "timestamp": datetime.datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        "portfolio_eod": eod_json_state
    }
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/EOD_State_{datetime.datetime.now().strftime('%Y%m%d')}.json", "w", encoding="utf-8") as f:
        json.dump(archive_json, f, ensure_ascii=False, indent=2)
    print("✅ EOD 结算全部完成！")
if __name__ == "__main__":
    run_eod_settlement()
