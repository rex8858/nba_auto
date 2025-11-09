# main.py — Hybrid v4.3-Live 晨間完整覆蓋版
# Author: Rextin Shelby & GPT-5 (NBA Auto Model v4.x)
# Last updated: 2025-11-09

import pandas as pd
import numpy as np
import datetime as dt
import pytz
import os
import hashlib
import random

# ───────────────────────────────────────────────
# 模型參數（預設）
SD_TEAM = 12
RHO = 0.40
DF_T = 5
N_SIM = 200_000

# ───────────────────────────────────────────────
# 輔助函數
def sha256sum(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def get_now_taipei():
    tz = pytz.timezone("Asia/Taipei")
    return dt.datetime.now(tz)

def simulate_scores(teamA_mean, teamB_mean):
    """Monte Carlo 模擬比賽分數"""
    cov = np.array([
        [SD_TEAM**2, RHO * SD_TEAM**2],
        [RHO * SD_TEAM**2, SD_TEAM**2]
    ])
    samples = np.random.multivariate_normal(
        [teamA_mean, teamB_mean], cov, size=N_SIM
    )
    return samples

# ───────────────────────────────────────────────
# 模型主體
def hybrid_v43_prediction(game_info):
    """輸入一場比賽資訊，輸出預測結果"""
    teamA = game_info["teamA"]
    teamB = game_info["teamB"]
    total_line = float(game_info["total"])
    spread_line = float(game_info["spread"])

    # 簡化模擬邏輯（實際版會調用多層模型）
    teamA_exp = random.uniform(100, 115)
    teamB_exp = random.uniform(100, 115)

    samples = simulate_scores(teamA_exp, teamB_exp)
    totals = samples[:, 0] + samples[:, 1]
    margins = samples[:, 0] - samples[:, 1]

    p_over = np.mean(totals > total_line)
    p_cover = np.mean(margins > -spread_line)

    ev_over = (p_over * 1.909) - (1 - p_over)
    ev_ats = (p_cover * 1.909) - (1 - p_cover)

    return {
        "teamA": teamA,
        "teamB": teamB,
        "total_line": total_line,
        "spread_line": spread_line,
        "prob_over": round(p_over, 3),
        "prob_cover": round(p_cover, 3),
        "EV_total": round(ev_over * 100, 2),
        "EV_ATS": round(ev_ats * 100, 2),
        "timestamp": get_now_taipei().strftime("%Y-%m-%d %H:%M:%S")
    }

# ───────────────────────────────────────────────
# CSV 安全寫入
def safe_append_to_csv(new_data, file_path, snapshot_type):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        # 移除舊 snapshot（相同比賽、相同 snapshot_type）
        df = df[
            ~(
                (df["game_id"].isin(new_data["game_id"])) &
                (df["snapshot_type"] == snapshot_type)
            )
        ]
        df = pd.concat([df, new_data], ignore_index=True)
    else:
        df = new_data

    df.to_csv(file_path, index=False)
    print(f"✅ Updated {file_path} ({len(new_data)} games added)")
    print(f"SHA256: {sha256sum(file_path)}")

# ───────────────────────────────────────────────
# 模擬 web 賽程抓取
def fetch_today_games():
    """模擬抓取今日 NBA 賽程"""
    # 實際可用 web 工具整合 NBA.com / ESPN
    now = get_now_taipei()
    base = now.replace(hour=7, minute=0, second=0, microsecond=0)
    games = []
    for i in range(10):
        games.append({
            "game_id": f"20251109_{i}",
            "teamA": f"Team{i+1}_A",
            "teamB": f"Team{i+1}_B",
            "tipoff": (base + dt.timedelta(minutes=i*30)),
            "spread": random.choice([-3.5, -2.5, -1.5, 1.5, 2.5, 3.5]),
            "total": random.choice([221.5, 224.5, 227.5, 230.5])
        })
    return pd.DataFrame(games)

# ───────────────────────────────────────────────
# 主任務流程
def main(snapshot_type="T60", window_min=15, window_max=45):
    now = get_now_taipei()
    df_games = fetch_today_games()
    upcoming = []

    for _, row in df_games.iterrows():
        minutes_to_tip = (row["tipoff"] - now).total_seconds() / 60
        if window_min <= minutes_to_tip <= window_max:
            upcoming.append(row)

    if not upcoming:
        print("⏸ No games within prediction window.")
        return

    preds = []
    for g in upcoming:
        pred = hybrid_v43_prediction(g)
        pred["game_id"] = g["game_id"]
        pred["snapshot_type"] = snapshot_type
        pred["snapshot_time"] = now.strftime("%Y-%m-%d %H:%M")
        preds.append(pred)

    df_new = pd.DataFrame(preds)
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    master_path = "data/NBA_AB_1030_1109_master_full_v43_TMC_with_summary.csv"
    pergame_path = "data/AB_per_game_1030_1109_v43_TMC.csv"

    safe_append_to_csv(df_new, master_path, snapshot_type)
    safe_append_to_csv(df_new, pergame_path, snapshot_type)

    print("\n✅ All updates complete.")
    print(f"Total {len(preds)} predictions added.")
    print(f"🕒 {now.strftime('%Y-%m-%d %H:%M:%S')} Asia/Taipei")

# ───────────────────────────────────────────────
# CLI 模式
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=str, default="morning")
    parser.add_argument("--snapshot", type=str, default="T60")
    parser.add_argument("--tz", type=str, default="Asia/Taipei")
    parser.add_argument("--window_min", type=int, default=15)
    parser.add_argument("--window_max", type=int, default=45)
    args = parser.parse_args()

    main(
        snapshot_type=args.snapshot,
        window_min=args.window_min,
        window_max=args.window_max
    )
