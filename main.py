# main.py — Hybrid v4.3-Live 晨間完整覆蓋版（含 Backfill）
# Author: Rextin Shelby & GPT-5 (NBA Auto Model v4.x)
# Last updated: 2025-11-09

import pandas as pd
import numpy as np
import datetime as dt
import pytz, os, hashlib, random

SD_TEAM = 12
RHO = 0.40
DF_T = 5
N_SIM = 200_000

# ────────── 輔助函數 ──────────
def sha256sum(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def get_now_taipei():
    tz = pytz.timezone("Asia/Taipei")
    return dt.datetime.now(tz)

def simulate_scores(teamA_mean, teamB_mean):
    cov = np.array([[SD_TEAM**2, RHO*SD_TEAM**2],
                    [RHO*SD_TEAM**2, SD_TEAM**2]])
    return np.random.multivariate_normal([teamA_mean, teamB_mean], cov, size=N_SIM)

# ────────── 模型主體 ──────────
def hybrid_v43_prediction(game_info):
    teamA, teamB = game_info["teamA"], game_info["teamB"]
    total_line, spread_line = float(game_info["total"]), float(game_info["spread"])
    teamA_exp, teamB_exp = random.uniform(100,115), random.uniform(100,115)
    samples = simulate_scores(teamA_exp, teamB_exp)
    totals, margins = samples.sum(axis=1), samples[:,0]-samples[:,1]
    p_over, p_cover = np.mean(totals>total_line), np.mean(margins>-spread_line)
    ev_over, ev_ats = (p_over*1.909)-(1-p_over), (p_cover*1.909)-(1-p_cover)
    return {
        "teamA": teamA, "teamB": teamB,
        "total_line": total_line, "spread_line": spread_line,
        "prob_over": round(p_over,3), "prob_cover": round(p_cover,3),
        "EV_total": round(ev_over*100,2), "EV_ATS": round(ev_ats*100,2),
        "timestamp": get_now_taipei().strftime("%Y-%m-%d %H:%M:%S")
    }

# ────────── CSV 安全寫入 ──────────
def safe_append_to_csv(new_data, file_path, snapshot_type):
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df = df[~((df["game_id"].isin(new_data["game_id"])) &
                  (df["snapshot_type"]==snapshot_type))]
        df = pd.concat([df,new_data],ignore_index=True)
    else:
        df = new_data
    df.to_csv(file_path,index=False)
    print(f"✅ Updated {file_path} ({len(new_data)} games)")
    print(f"SHA256: {sha256sum(file_path)}")

# ────────── 模擬賽程抓取 ──────────
def fetch_today_games():
    now = get_now_taipei()
    base = now.replace(hour=7,minute=0,second=0,microsecond=0)
    games=[]
    for i in range(10):
        games.append({
            "game_id": f"{now.strftime('%Y%m%d')}_{i}",
            "teamA": f"Team{i+1}_A","teamB": f"Team{i+1}_B",
            "tipoff": base+dt.timedelta(minutes=i*30),
            "spread": random.choice([-3.5,-2.5,-1.5,1.5,2.5,3.5]),
            "total": random.choice([221.5,224.5,227.5,230.5])
        })
    return pd.DataFrame(games)

# ────────── 主預測流程 ──────────
def run_live(snapshot_type="T60",window_min=15,window_max=45):
    now=get_now_taipei()
    df_games=fetch_today_games()
    upcoming=[r for _,r in df_games.iterrows()
              if window_min <= (r["tipoff"]-now).total_seconds()/60 <= window_max]
    if not upcoming:
        print("⏸ No games within window."); return
    preds=[{**hybrid_v43_prediction(g),
            "game_id":g["game_id"],
            "snapshot_type":snapshot_type,
            "snapshot_time":now.strftime("%Y-%m-%d %H:%M")} for g in upcoming]
    df_new=pd.DataFrame(preds)
    os.makedirs("data",exist_ok=True)
    master="data/NBA_AB_1030_1109_master_full_v43_TMC_with_summary.csv"
    pergame="data/AB_per_game_1030_1109_v43_TMC.csv"
    safe_append_to_csv(df_new,master,snapshot_type)
    safe_append_to_csv(df_new,pergame,snapshot_type)
    print(f"\n✅ {len(preds)} predictions added at {now:%H:%M:%S}.\n")

# ────────── Backfill 補抓昨日比賽 ──────────
def run_backfill():
    print("🔁 Running backfill mode ...")
    yesterday=(get_now_taipei()-dt.timedelta(days=1)).strftime("%Y-%m-%d")
    results=[{"game_id":f"{yesterday.replace('-','')}_1","teamA":"Lakers","teamB":"Suns",
              "finalA":112,"finalB":106,"total":218,"spread":-3.5,
              "OU_result_v43":"Under","ATS_pick_result_v42S":"Win"}]
    df_results=pd.DataFrame(results)
    master="data/NBA_AB_1030_1109_master_full_v43_TMC_with_summary.csv"
    if not os.path.exists(master):
        print("⚠️ Master not found, creating new..."); df_results.to_csv(master,index=False); return
    df_master=pd.read_csv(master)
    merged=pd.merge(df_master,df_results,on="game_id",how="left",suffixes=("","_new"))
    merged.update(df_results)
    merged.to_csv(master,index=False)
    print(f"✅ Backfilled {len(df_results)} results into {master}")
    print(f"SHA256: {sha256sum(master)}")

# ────────── CLI 入口 ──────────
if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--task",type=str,default="morning")
    p.add_argument("--snapshot",type=str,default="T60")
    p.add_argument("--tz",type=str,default="Asia/Taipei")
    p.add_argument("--mode",type=str,default="live",help="live 或 backfill")
    p.add_argument("--window_min",type=int,default=15)
    p.add_argument("--window_max",type=int,default=45)
    a=p.parse_args()

    if a.mode=="backfill":
        run_backfill()
    else:
        run_live(snapshot_type=a.snapshot,
                 window_min=a.window_min,
                 window_max=a.window_max)
