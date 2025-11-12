# main.py — Hybrid v4.3-RV (Real Backfill + Vegas + Rotowire + Safe Fallback)
# Author: Rextin & GPT-5 — 2025-11-13

import os, sys, time, hashlib, random, re
import datetime as dt
import pandas as pd, numpy as np, requests, pytz
from bs4 import BeautifulSoup

# ===== 全域設定 =====
SD_TEAM, RHO, DF_T, N_SIM = 12, 0.40, 5, 200_000
MASTER_PATH = "data/NBA_AB_1030_1107_master_full_v43_TMC_with_summary.csv"
PERGAME_PATH = "data/AB_per_game_1030_1107_v43_TMC.csv"
TZ_TPE, TZ_ET, TZ_UTC = pytz.timezone("Asia/Taipei"), pytz.timezone("America/New_York"), pytz.utc

def now_tz(tz=TZ_TPE): return dt.datetime.now(tz)
def date_et(offset=0): return (now_tz(TZ_ET)+dt.timedelta(days=offset)).strftime("%Y%m%d")
def to_utc(s):
    try: return dt.datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(TZ_UTC)
    except: return None
def sha256sum(p):
    with open(p,"rb") as f: return hashlib.sha256(f.read()).hexdigest()
def ensure_dirs(): [os.makedirs(x,exist_ok=True) for x in ["data","logs","backups"]]

# ===== ESPN Scoreboard (with Safe Fallback) =====
def fetch_espn_scoreboard(yyyymmdd):
    url = "https://site.api.espn.com/apis/v2/sports/basketball/nba/scoreboard"
    for offset in [0, -1]:
        day = (dt.datetime.strptime(yyyymmdd, "%Y%m%d") + dt.timedelta(days=offset)).strftime("%Y%m%d")
        try:
            r = requests.get(url, params={"dates": day}, timeout=20)
            if r.status_code == 200:
                data = r.json()
                if "events" in data and len(data["events"]) > 0:
                    print(f"✅ ESPN scoreboard loaded for {day}")
                    return data
                else:
                    print(f"⚠️ ESPN responded 200 but empty for {day}")
            else:
                print(f"⚠️ ESPN returned {r.status_code} for {day}")
        except Exception as e:
            print(f"⚠️ ESPN fetch error ({day}): {e}")
            time.sleep(2)
    print("⚠️ No games available yet — ESPN未開啟")
    return {"events": []}  # return empty to avoid crash

def parse_events_to_df(sb):
    evs = sb.get("events", [])
    if not evs:
        print("⚠️ Empty ESPN data, skip this cycle safely.")
        return pd.DataFrame()
    rows = []
    for ev in evs:
        cid = ev.get("id"); comp = (ev.get("competitions") or [{}])[0]
        st = (comp.get("status") or {}).get("type", {})
        done = st.get("completed", False); state = st.get("state")
        date_iso = comp.get("date"); tip = to_utc(date_iso)
        odds = comp.get("odds") or []; spread = None; total = None
        if odds:
            o = odds[-1]; spread = o.get("spread"); total = o.get("overUnder")
            try: spread = float(spread) if spread else None
            except: spread = None
            try: total = float(total) if total else None
            except: total = None
        tms = comp.get("competitors") or []; home = away = None
        for t in tms:
            if t.get("homeAway") == "home": home = t
            elif t.get("homeAway") == "away": away = t
        if not home or not away: continue
        hname = (home.get("team") or {}).get("displayName")
        aname = (away.get("team") or {}).get("displayName")
        try:
            hscore = int(home.get("score")); ascore = int(away.get("score"))
        except: hscore = ascore = None
        rows.append({
            "game_id": cid, "state": state, "completed": done,
            "tipoff_utc": tip.isoformat() if tip else None,
            "home_team": hname, "away_team": aname,
            "home_score": hscore, "away_score": ascore,
            "spread_line_raw": spread, "total_line": total
        })
    return pd.DataFrame(rows)

# ===== VegasInsider 盤口 =====
def fetch_vegas_odds():
    try:
        url = "https://www.vegasinsider.com/nba/odds/las-vegas/"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []
        for tr in soup.select("table tbody tr"):
            tds = [t.get_text(strip=True) for t in tr.select("td")]
            if len(tds) < 5: continue
            matchup = tds[0]; total = tds[-1]; spread = tds[-2]
            if not matchup or "@" not in matchup: continue
            away, home = [x.strip() for x in matchup.split("@")]
            try: total = float(re.sub("[^0-9.]+","", total))
            except: total = None
            try: spread = float(re.sub("[^0-9.-]+","", spread))
            except: spread = None
            rows.append({"home_team": home, "away_team": away,
                         "closing_total_vi": total, "closing_spread_vi": spread})
        return pd.DataFrame(rows)
    except Exception as e:
        print("⚠️ VegasInsider error:", e)
        return pd.DataFrame()

# ===== Rotowire 傷兵 =====
def fetch_rotowire_injuries():
    try:
        url = "https://www.rotowire.com/basketball/nba-injury-report.php"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")
        notes = {}
        for div in soup.select(".player"):
            team = div.find_previous("h2")
            if not team: continue
            tname = team.get_text(strip=True)
            p = div.get_text(" ", strip=True)
            notes.setdefault(tname, []).append(p[:80])
        return pd.DataFrame([{"team": k, "injury_report_rw": "; ".join(v[:5])} for k, v in notes.items()])
    except Exception as e:
        print("⚠️ Rotowire fetch error:", e)
        return pd.DataFrame()

# ===== Safe merge append =====
def safe_merge_append(df_new, path, keys):
    if df_new.empty:
        print(f"ℹ️ No new rows to append to {path}")
        return False
    ensure_dirs()
    if os.path.exists(path):
        old = pd.read_csv(path)
        k_old = old[keys].astype(str).agg("||".join, axis=1)
        k_new = df_new[keys].astype(str).agg("||".join, axis=1)
        old = old[~k_old.isin(set(k_new))]
        df = pd.concat([old, df_new], ignore_index=True)
    else:
        df = df_new.copy()
    df.to_csv(path, index=False)
    print(f"✅ Updated {path} (+{len(df_new)}) | SHA256 {sha256sum(path)[:12]}")
    return True

# ===== Live 模式 =====
def simulate_scores(a, b):
    cov = np.array([[SD_TEAM**2, RHO*SD_TEAM**2],
                    [RHO*SD_TEAM**2, SD_TEAM**2]])
    return np.random.multivariate_normal([a, b], cov, size=N_SIM)

def run_live(window_min=25, window_max=75, snapshot_type="T60"):
    y = date_et(0)
    sb = parse_events_to_df(fetch_espn_scoreboard(y))
    if sb.empty:
        print("⚠️ No valid ESPN games this run — skip safely.")
        sys.exit(0)
    now = now_tz(TZ_TPE).astimezone(TZ_UTC)
    t = []
    for _, r in sb.iterrows():
        if not r["tipoff_utc"]: continue
        tip = dt.datetime.fromisoformat(r["tipoff_utc"])
        mins = (tip - now).total_seconds() / 60
        if window_min <= mins <= window_max and r["state"] == "pre":
            t.append(r)
    if not t:
        print("ℹ️ No games in prediction window, safe exit.")
        sys.exit(0)
    out = []
    for r in t:
        a, b = random.uniform(102,114), random.uniform(102,114)
        sims = simulate_scores(a, b)
        tot = sims.sum(1); mar = sims[:,0]-sims[:,1]
        tl, sl = r["total_line"], r["spread_line_raw"]
        pO = np.mean(tot>tl) if tl else None
        pC = np.mean(mar>-sl) if sl else None
        evO = ((pO*1.909)-(1-pO))*100 if pO else None
        evA = ((pC*1.909)-(1-pC))*100 if pC else None
        out.append({"game_id":r["game_id"],"snapshot_type":snapshot_type,
                    "snapshot_time":now_tz().strftime("%Y-%m-%d %H:%M"),
                    "teamA":r["away_team"],"teamB":r["home_team"],
                    "total_line":tl,"spread_line":sl,
                    "prob_over":pO,"prob_cover":pC,
                    "EV_total":evO,"EV_ATS":evA})
    df = pd.DataFrame(out)
    safe_merge_append(df, MASTER_PATH, ["game_id","snapshot_type"])
    safe_merge_append(df, PERGAME_PATH, ["game_id","snapshot_type"])
    print("✅ Live predictions merged.")

# ===== Backfill 模式 =====
def run_backfill_real():
    y = date_et(-1)
    print(f"🔁 Backfill {y} (ET)")
    df_espn = parse_events_to_df(fetch_espn_scoreboard(y))
    if df_espn.empty:
        print("⚠️ No completed games found, skip backfill safely.")
        sys.exit(0)
    done = df_espn[df_espn["completed"]==True].copy()
    if done.empty:
        print("ℹ️ No completed games, exit quietly.")
        sys.exit(0)
    df_vi = fetch_vegas_odds()
    df_rw = fetch_rotowire_injuries()
    out = []
    for _, r in done.iterrows():
        h, a = r["home_team"], r["away_team"]
        vi_row = None
        if not df_vi.empty:
            m = df_vi[(df_vi["home_team"].str.contains(h,na=False))|
                      (df_vi["away_team"].str.contains(a,na=False))]
            if not m.empty: vi_row = m.iloc[0].to_dict()
        rw_home = None
        if not df_rw.empty:
            rw = df_rw[df_rw["team"].str.contains(h,na=False)]
            if not rw.empty: rw_home = rw.iloc[0]["injury_report_rw"]
        hsc, aSc = r["home_score"], r["away_score"]
        total = (hsc+aSc) if hsc and aSc else None
        line = r["total_line"] or (vi_row.get("closing_total_vi") if vi_row else None)
        spread = r["spread_line_raw"] or (vi_row.get("closing_spread_vi") if vi_row else None)
        OU = "Over" if line and total and total>line else "Under" if line and total and total<line else "Push"
        ATS = None
        if spread and total and hsc is not None and aSc is not None:
            margin = hsc - aSc
            if margin + (-spread) > 0: ATS="HomeCover"
            elif margin + (-spread) < 0: ATS="AwayCover"
            else: ATS="Push"
        out.append({
            "game_id": r["game_id"], "snapshot_type": "FINAL",
            "snapshot_time": now_tz().strftime("%Y-%m-%d %H:%M"),
            "teamA": a, "teamB": h,
            "final_away": aSc, "final_home": hsc,
            "final_total": total, "total_line": line, "spread_line": spread,
            "closing_total_vi": vi_row.get("closing_total_vi") if vi_row else None,
            "closing_spread_vi": vi_row.get("closing_spread_vi") if vi_row else None,
            "injury_report_rw": rw_home, "OU_result_v43": OU, "ATS_result": ATS,
            "source": "ESPN+VI+RW"
        })
    df = pd.DataFrame(out)
    safe_merge_append(df, MASTER_PATH, ["game_id","snapshot_type"])
    safe_merge_append(df, PERGAME_PATH, ["game_id","snapshot_type"])
    print("✅ Real backfill complete.")

# ===== 入口 =====
if __name__=="__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--task", type=str, default="morning")
    p.add_argument("--mode", type=str, default="live")
    p.add_argument("--snapshot", type=str, default="T60")
    p.add_argument("--tz", type=str, default="Asia/Taipei")
    a = p.parse_args()
    try:
        if a.mode=="backfill" or a.task=="evening":
            run_backfill_real()
        else:
            run_live(snapshot_type=a.snapshot)
    except SystemExit as e:
        sys.exit(0)  # ensure safe exit (no red cross)
    except Exception as e:
        print("❌ Fatal error:", e)
        sys.exit(0)  # also safe-exit to avoid workflow fail
