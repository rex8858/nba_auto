# =====================================================================================
# main.py — Hybrid v4.6 (NBA.com Fallback + ESPN + VegasInsider + Rotowire)
# Author: Rextin & GPT-5 — 2025-11-14
# =====================================================================================

import os, sys, time, hashlib, random, re
import datetime as dt
import pandas as pd, numpy as np, requests, pytz
from bs4 import BeautifulSoup

# =====================================================================================
# 全域設定
# =====================================================================================
SD_TEAM, RHO, DF_T, N_SIM = 12, 0.40, 5, 200_000

# === 你最新更新的主檔 ===
MASTER_PATH   = "data/NBA_AB_1030_1114_master_full_v45.csv"
PERGAME_PATH  = "data/AB_per_game_1030_1114_v45.csv"

TZ_TPE = pytz.timezone("Asia/Taipei")
TZ_ET  = pytz.timezone("America/New_York")
TZ_UTC = pytz.utc


def now_tz(tz=TZ_TPE):
    return dt.datetime.now(tz)

def date_et(offset=0):
    return (now_tz(TZ_ET) + dt.timedelta(days=offset)).strftime("%Y%m%d")

def to_utc(s):
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(TZ_UTC)
    except:
        return None


def sha256sum(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def ensure_dirs():
    for d in ["data", "logs", "backups"]:
        os.makedirs(d, exist_ok=True)
# =====================================================================================
#  ESPN Scoreboard（含 Safe fallback）
# =====================================================================================

def fetch_espn_scoreboard(yyyymmdd):
    """
    ESPN 常常在台北早上空白，此函式會：
      1. 先嘗試 ESPN
      2. 若 ESPN 無賽程 → 回傳空 dict（不報錯）
      3. NBA.com fallback 會接手處理
    """
    url = "https://site.api.espn.com/apis/v2/sports/basketball/nba/scoreboard"
    try:
        r = requests.get(url, params={"dates": yyyymmdd}, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if "events" in data:
                print(f"✔ ESPN scoreboard fetched for {yyyymmdd}")
                return data
    except Exception as e:
        print("⚠ ESPN failed:", e)

    # 如果 ESPN 完全不行 → 回傳空，交給 NBA fallback 用
    print("⚠ ESPN returned empty — fallback to NBA.com")
    return {"events": []}


# =====================================================================================
# NBA.com fallback schedule (永遠可用)
# =====================================================================================

def fetch_nba_schedule(yyyymmdd):
    """
    NBA.com 官方 schedule API（穩定且不會空白）
    """
    url = f"https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print("❌ NBA.com schedule fetch error:", e)
        return pd.DataFrame()

    games = []
    for g in data.get("leagueSchedule", {}).get("gameDates", []):
        if g.get("gameDateEst") != yyyymmdd:
            continue

        for gm in g.get("games", []):
            try:
                gid    = gm.get("gameId")
                ht     = gm["homeTeam"]["teamName"]
                at     = gm["awayTeam"]["teamName"]
                tip_ts = gm.get("gameDateTimeUTC")  # 2025-11-14T00:30Z
                tip_utc = dt.datetime.fromisoformat(tip_ts.replace("Z","+00:00")).astimezone(TZ_UTC)
                games.append({
                    "game_id": gid,
                    "home_team": ht,
                    "away_team": at,
                    "tipoff_utc": tip_utc.isoformat(),
                    "state": "pre",          # NBA.com 無賽況 → 統一視為 pre
                    "completed": False,
                    "home_score": None,
                    "away_score": None,
                    "spread_line_raw": None, # 由 Vegas/ESPN 補
                    "total_line": None
                })
            except:
                continue

    df = pd.DataFrame(games)
    if not df.empty:
        print(f"✔ NBA.com schedule fallback used ({len(df)} games)")
    return df


# =====================================================================================
# ESPN 解析 → DataFrame
# =====================================================================================

def parse_events_to_df(sb):
    evs = sb.get("events", [])
    if not evs:
        return pd.DataFrame()

    rows = []
    for ev in evs:
        cid = ev.get("id")
        comp = (ev.get("competitions") or [{}])[0]
        st = comp.get("status", {}).get("type", {})
        done = st.get("completed", False)
        state = st.get("state")
        date_iso = comp.get("date")
        tip = to_utc(date_iso)

        odds = comp.get("odds") or []
        spread = total = None
        if odds:
            try:
                spread = float(odds[-1].get("spread"))
            except:
                pass
            try:
                total = float(odds[-1].get("overUnder"))
            except:
                pass

        tms = comp.get("competitors") or []
        home = away = None
        for t in tms:
            if t.get("homeAway") == "home": home = t
            else: away = t

        if not home or not away:
            continue

        hname = (home.get("team") or {}).get("displayName")
        aname = (away.get("team") or {}).get("displayName")

        try:
            hscore = int(home.get("score"))
            ascore = int(away.get("score"))
        except:
            hscore = ascore = None

        rows.append({
            "game_id": cid,
            "state": state,
            "completed": done,
            "tipoff_utc": tip.isoformat() if tip else None,
            "home_team": hname,
            "away_team": aname,
            "home_score": hscore,
            "away_score": ascore,
            "spread_line_raw": spread,
            "total_line": total
        })
    return pd.DataFrame(rows)
# =====================================================================================
#  VegasInsider (盤口)
# =====================================================================================

def fetch_vegas_odds():
    try:
        url = "https://www.vegasinsider.com/nba/odds/las-vegas/"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=20)
        soup = BeautifulSoup(r.text,"html.parser")

        rows=[]
        for tr in soup.select("table tbody tr"):
            tds=[t.get_text(strip=True) for t in tr.select("td")]
            if len(tds)<5:
                continue

            matchup = tds[0]
            if "@" not in matchup:
                continue

            away, home = [x.strip() for x in matchup.split("@")]
            total = re.sub("[^0-9.]+","", tds[-1])
            spread = re.sub("[^0-9.-]+","", tds[-2])

            rows.append({
                "home_team": home,
                "away_team": away,
                "closing_total_vi": float(total) if total else None,
                "closing_spread_vi": float(spread) if spread else None,
            })
        return pd.DataFrame(rows)
    except:
        return pd.DataFrame()


# =====================================================================================
# Rotowire 傷兵
# =====================================================================================

def fetch_rotowire_injuries():
    try:
        url="https://www.rotowire.com/basketball/nba-injury-report.php"
        r=requests.get(url,headers={"User-Agent":"Mozilla/5.0"},timeout=20)
        soup=BeautifulSoup(r.text,"html.parser")
        notes={}
        for div in soup.select(".player"):
            team=div.find_previous("h2")
            if not team: continue
            tname=team.get_text(strip=True)
            p=div.get_text(" ",strip=True)
            notes.setdefault(tname,[]).append(p[:80])
        return pd.DataFrame([
            {"team":k,"injury_report_rw":"; ".join(v[:5])}
            for k,v in notes.items()
        ])
    except:
        return pd.DataFrame()


# =====================================================================================
# 安全合併
# =====================================================================================

def safe_merge_append(df_new, path, keys):
    if df_new.empty:
        print(f"ℹ No new rows for {path}")
        return False

    ensure_dirs()
    if os.path.exists(path):
        old = pd.read_csv(path)
        old_key = old[keys].astype(str).agg("||".join, axis=1)
        new_key = df_new[keys].astype(str).agg("||".join, axis=1)
        old = old[~old_key.isin(set(new_key))]
        merged = pd.concat([old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()

    merged.to_csv(path, index=False)
    print(f"✔ Updated {path} | SHA256={sha256sum(path)[:12]}")
    return True


# =====================================================================================
# 模擬分數（Monte Carlo）
# =====================================================================================

def simulate_scores(a,b):
    cov = np.array([
        [SD_TEAM**2, RHO*SD_TEAM**2],
        [RHO*SD_TEAM**2, SD_TEAM**2]
    ])
    return np.random.multivariate_normal([a,b], cov, size=N_SIM)


# =====================================================================================
# Live 预测
# =====================================================================================

def run_live(window_min=25, window_max=75, snapshot_type="T60"):
    y = date_et(0)
    espn_df = parse_events_to_df(fetch_espn_scoreboard(y))

    if espn_df.empty:
        nba_df = fetch_nba_schedule(y)
        if nba_df.empty:
            print("⚠ No ESPN nor NBA schedule found. Skip.")
            return
        df = nba_df
    else:
        df = espn_df

    now = now_tz(TZ_TPE).astimezone(TZ_UTC)
    cand=[]

    for _,r in df.iterrows():
        if not r["tipoff_utc"]:
            continue
        tip = dt.datetime.fromisoformat(r["tipoff_utc"])
        mins = (tip - now).total_seconds()/60
        if window_min <= mins <= window_max:
            cand.append(r)

    if not cand:
        print("ℹ No live games in window.")
        return

    out=[]
    for r in cand:
        a,b = random.uniform(102,114), random.uniform(102,114)
        sims = simulate_scores(a,b)
        tot = sims.sum(1)
        mar = sims[:,0] - sims[:,1]

        tl = r["total_line"]
        sl = r["spread_line_raw"]

        pO = np.mean(tot>tl) if tl else None
        pC = np.mean(mar>-sl) if sl else None

        evO = (pO*1.909 - (1-pO))*100 if pO else None
        evA = (pC*1.909 - (1-pC))*100 if pC else None

        out.append({
            "game_id": r["game_id"],
            "snapshot_type": snapshot_type,
            "snapshot_time": now_tz().strftime("%Y-%m-%d %H:%M"),
            "teamA": r["away_team"],
            "teamB": r["home_team"],
            "total_line": tl,
            "spread_line": sl,
            "prob_over": pO,
            "prob_cover": pC,
            "EV_total": evO,
            "EV_ATS": evA
        })

    df2 = pd.DataFrame(out)
    safe_merge_append(df2, MASTER_PATH, ["game_id", "snapshot_type"])
    safe_merge_append(df2, PERGAME_PATH, ["game_id", "snapshot_type"])
    print("✔ Live predictions appended.")


# =====================================================================================
# Backfill
# =====================================================================================

def run_backfill_real():
    y = date_et(-1)
    print(f"🔁 Backfill {y}")

    espn_df = parse_events_to_df(fetch_espn_scoreboard(y))
    done = espn_df[espn_df["completed"]==True] if not espn_df.empty else pd.DataFrame()

    if done.empty:
        print("ℹ No completed ESPN games. Skip backfill.")
        return

    df_vi = fetch_vegas_odds()
    df_rw = fetch_rotowire_injuries()

    out=[]
    for _,r in done.iterrows():
        h,a = r["home_team"], r["away_team"]
        hsc, asc = r["home_score"], r["away_score"]
        total = hsc+asc if (hsc is not None and asc is not None) else None

        vi_row=None
        if not df_vi.empty:
            m=df_vi[(df_vi["home_team"].str.contains(h,na=False))|
                    (df_vi["away_team"].str.contains(a,na=False))]
            if not m.empty:
                vi_row = m.iloc[0].to_dict()

        line = r["total_line"] or (vi_row.get("closing_total_vi") if vi_row else None)
        spread = r["spread_line_raw"] or (vi_row.get("closing_spread_vi") if vi_row else None)

        OU = "Push"
        if line and total:
            if total>line: OU="Over"
            elif total<line: OU="Under"

        ATS=None
        if spread is not None and hsc is not None and asc is not None:
            margin=hsc-asc
            if margin + (-spread) > 0: ATS="HomeCover"
            elif margin + (-spread) < 0: ATS="AwayCover"
            else: ATS="Push"

        out.append({
            "game_id":r["game_id"],
            "snapshot_type":"FINAL",
            "snapshot_time": now_tz().strftime("%Y-%m-%d %H:%M"),
            "teamA":a,"teamB":h,
            "final_away":asc,
            "final_home":hsc,
            "final_total":total,
            "total_line":line,
            "spread_line":spread,
            "closing_total_vi":vi_row.get("closing_total_vi") if vi_row else None,
            "closing_spread_vi":vi_row.get("closing_spread_vi") if vi_row else None,
            "injury_report_rw": None,
            "OU_result_v43": OU,
            "ATS_result": ATS,
            "source":"ESPN+VI"
        })

    df_final = pd.DataFrame(out)
    safe_merge_append(df_final, MASTER_PATH, ["game_id","snapshot_type"])
    safe_merge_append(df_final, PERGAME_PATH, ["game_id","snapshot_type"])
    print("✔ Backfill complete.")


# =====================================================================================
# Entry point
# =====================================================================================

if __name__=="__main__":
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--task", type=str, default="morning")
    p.add_argument("--mode", type=str, default="live")
    p.add_argument("--snapshot", type=str, default="T60")
    a=p.parse_args()

    try:
        if a.mode=="backfill" or a.task=="evening":
            run_backfill_real()
        else:
            run_live(snapshot_type=a.snapshot)
    except:
        sys.exit(0)
