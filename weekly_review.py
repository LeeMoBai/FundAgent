import os
import json
import datetime
import pytz
from google import genai
import requests

# ==========================================
# 1. 捞取最近 7 天的战略 JSON 底稿
# ==========================================
def gather_weekly_json_logs() -> str:
    log_dir = "logs"
    if not os.path.exists(log_dir):
        return "[]"
        
    tz_bj = pytz.timezone('Asia/Shanghai')
    today = datetime.datetime.now(tz_bj)
    
    # 计算过去 7 天的日期字符串列表 (例如 '2026-02-25')
    recent_dates = [(today - datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    
    weekly_data = []
    
    # 遍历 logs 文件夹
    for filename in sorted(os.listdir(log_dir)):
        if filename.endswith(".json"):
            # 检查文件名的前缀日期是否在最近 7 天内
            if any(filename.startswith(date_str) for date_str in recent_dates):
                filepath = os.path.join(log_dir, filename)
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        weekly_data.append({
                            "source_file": filename,
                            "content": data
                        })
                except Exception as e:
                    print(f"读取 {filename} 失败: {e}")
                    
    # 把这 7 天所有的 JSON 压平成一个巨大的字符串
    return json.dumps(weekly_data, ensure_ascii=False, indent=2)

# ==========================================
# 2. 唤醒 V4.0 大脑进行深度数据挖掘
# ==========================================
def ask_v4_review_agent(weekly_json_str: str) -> str:
    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    
    if weekly_json_str == "[]" or len(weekly_json_str) < 50:
        return "数据湖中暂无本周 JSON 记录，无法生成复盘。"
        
    prompt = f"""
# Role: V4.0 首席量化风控官 (Chief Risk Officer)
现在是周末复盘时间。请阅读下方由系统自动采集的【本周所有盘中与盘后 JSON 底稿】。
这些数据包含了我们一整周的宏观锚点、盘中AI推演、你的实际交易动作，以及雷达池的沉寂标的。

【本周量化数据湖】：
```json
{weekly_json_str}
Output Format:
请基于上述 JSON，进行冷酷、客观的交叉比对，输出《周度量化体检报告》：

📊 [本周盈亏与纪律体检]
(对比 EOD 盘后记录的【实际交易动作】与 Midday 盘中的【AI 建议】，评价主人本周的纪律执行度：是知行合一，还是被情绪裹挟？)

🔍 [AI 预测胜率与证伪打脸]
(提取本周前几天 AI 设定的【证伪条件】或【预测】，对比本周后几天的【实际行情】，客观评价 AI 逻辑的胜率。如果有错，错在哪里？)

👻 [雷达坟场与踏空成本]
(分析 JSON 中的 radar_graveyard。有没有哪个标的其实在偷偷异动/缩量，但因为没触发绝对扳机被我们错过了？)

⚔️ [下周宏观定调与火力部署]
(根据 5 日序列中 US10Y、BTC、DXY 的走势，为下周的 A股/半导体 定调，并给出下周的重点防线提示。)
"""

    print("   [+] 正在吞噬并计算本周海量 JSON 数据...")
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt, 
        config=genai.types.GenerateContentConfig(temperature=0.2) # 复盘需要稍微发散一点思维，温度设为 0.2
    )
    return response.text

# ==========================================
# 3. 企微推送
# ==========================================
def send_wechat_weekly_report(content: str):
    robot_key = os.environ.get("WECHAT_ROBOT_KEY")
    if not robot_key:
        print("⚠️ 未配置 WECHAT_ROBOT_KEY。")
        return

    payload = {
        "msgtype": "markdown", 
        "markdown": {
            "content": f"<font color='warning'>**👑 V4.0 周度量化体检报告**</font>\n\n{content}"
        }
    }
    try:
        url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={robot_key}"
        requests.post(url, json=payload)
        print("📡 周度复盘报告企微推送成功！")
    except Exception as e:
        print(f"❌ 企微推送异常: {e}")

# ==========================================
# 主流程
# ==========================================
if __name__ == "__main__":  # 修复点：添加双下划线
    print("🚀 [V4.0 Weekend Review] 启动周末复盘大脑...")
    try:
        json_data = gather_weekly_json_logs()
        print(f"📦 成功捞取本周 JSON 数据片段，准备分析...")

        review_report = ask_v4_review_agent(json_data)
        send_wechat_weekly_report(review_report)
        
        print("🎉 V4.0 周末复盘执行完毕！")
    except Exception as e:
        print(f"❌ 运行失败: {e}")
        raise e
