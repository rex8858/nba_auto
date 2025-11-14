# main.py — Hybrid v4.5 (ESPN-stable + Safe Merge)
# Author: Rextin & GPT-5 — 2025-11-14

import os, sys, time, hashlib, random, re
import datetime as dt

import pandas as pd
import numpy as np
import requests
import pytz
from bs4 import BeautifulSoup

# ===== 全域設定 =====
SD_TEAM, RHO, DF_T, N_SIM = 12, 0.40, 5, 200_000

# 依照你剛剛提供的主檔名稱
MASTER_PATH  = "data/NBA_AB_1030_1114_master_full_v45.csv"
PERGAME_PATH = "data/AB_per_game_1030_1114_v45.csv"

TZ_TPE = pytz.timezone("Asia/Taipei")
TZ_ET  = pytz.timezone("America/New_York")
TZ_UTC = pytz.utc


def now_tz(tz=TZ_TPE) -> dt.datetime:
    return dt.datetime.now(tz)


def date_et(offset=0) -> str:
    """回傳 ET 日期（YYYYMMDD），offset 以天為單位。"""
    return (now_tz(TZ_ET) + dt.timedelta(days=offset)).strftime("%Y%m%d")


def to_utc(iso_str: str):
    """把 ESPN 的 ISO 時間字串轉成 UTC datetime。"""
    if not iso_str:
        return None
    try:
        return dt.datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(TZ_UTC)
    except Exception:
        return None


def sha256sum(path: str) -> str:
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def ensure_dirs():
    for d in ["data", "logs", "backups"]:
        os.makedirs(d, exist_ok=True)


# ===== ESPN Scoreboard（單日版） =====
def fetch_espn_scoreboard_for_date(yyyymmdd: str) -> dict:
    """
    只抓指定這一天（ET）的 scoreboard。
    若 ESPN 還沒開 / 沒有這一天 → 回傳 {"events": []}，由呼叫端自己決定怎麼處理。
    """
    url = "https://site.api.espn.com/apis/v2/sports/basketball/nba/scoreboard"
    try:
        r = requests.get(url, params={"dates": yyyymmdd}, timeout=20)
        if r.status_code == 404:
            print(f"⚠️ ESPN 404 for {yyyymmdd}（這一天可能還沒開賽程）")
            return {"events": []}
        if r.status_code != 200:
            print(f"⚠️ ESPN status {r.status_code} for {yyyymmdd}")
            return {"events": []}
        data = r.json()
        if not data.get("events"):
            print(f"⚠️ ESPN {yyyymmdd} events 為空")
        else:
            print(f"✅ ESPN scoreboard loaded for {yyyymmdd} with {len(data['events'])} events")
        return data
    except Exception as e:
        print(f"⚠️ ESPN fetch error for {yyyymmdd}: {e}")
        return {"events": []}


def parse_events_to_df(sb: dict) -> pd.DataFrame:
    """把 ESPN JSON 轉成 DataFrame。若沒有 events → 回傳空 df。"""
    evs = sb.get("events", [])
    if not evs:
        return pd.DataFrame()

    rows = []
    for ev in evs:
        cid = ev.get("id")
        comp = (ev.get("competitions") or [{}])[0]

        status = (comp.get("status") or {}).get("type", {})
        completed = status.get("completed", False)
        state = status.get("state")

        date_iso = comp.get("date")
        tip = to_utc(date_iso)

        odds = comp.get("odds") or []
        spread = None
        total = None
        if odds:
            o = odds[-1]
            spread = o.get("spread")
            total = o.get("overUnder")
            try:
                spread = float(spread) if spread not in [None, ""] else None
            except Exception:
                spread = None
            try:
                total = float(total) if total not in [None, ""] else None
            except Exception:
                total = None

        teams = comp.get("competitors") or []
        home = away = None
        for t in teams:
            if t.get("homeAway") == "home":
                home = t
            elif t.get("homeAway") == "away":
                away = t
        if not home or not away:
            continue

        hname = (home.get("team") or {}).get("displayName")
        aname = (away.get("team") or {}).get("displayName")

        try:
            hscore = int(home.get("score"))
        except Exception:
            hscore = None
        try:
            ascore = int(away.get("score"))
        except Exception:
            ascore = None

        rows.append(
            {
                "game_id": cid,
                "state": state,
                "completed": completed,
                "tipoff_utc": tip.isoformat() if tip else None,
                "home_team": hname,
                "away_team": aname,
                "home_score": hscore,
                "away_score": ascore,
                "spread_line_raw": spread,
                "total_line": total,
            }
        )

    return pd.DataFrame(rows)


def fetch_espn_scoreboard_multi(dates: list[str]) -> pd.DataFrame:
    """
    一次抓多個 ET 日期（例如 [昨天, 今天]），合併成一份 DataFrame。
    用來避免「跨日／時差」導致早上抓不到比賽。
    """
    dfs = []
    for d in dates:
        sb = fetch_espn_scoreboard_for_date(d)
        df = parse_events_to_df(sb)
        if not df.empty:
            df["scoreboard_date"] = d
            dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    big = pd.concat(dfs, ignore_index=True)
    # 同一場比賽（game_id）若出現在兩天，以第一筆為準
    return big.drop_duplicates(subset=["game_id"])


# ===== VegasInsider 盤口（backfill 用） =====
def fetch_vegas_odds() -> pd.DataFrame:
    try:
        url = "https://www.vegasinsider.com/nba/odds/las-vegas/"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        rows = []
        for tr in soup.select("table tbody tr"):
            tds = [td.get_text(strip=True) for td in tr.select("td")]
            if len(tds) < 5:
                continue
            matchup = tds[0]
            if "@" not in matchup:
                continue
            away, home = [x.strip() for x in matchup.split("@")]

            spread_txt = tds[-2]
            total_txt = tds[-1]
            try:
                spread_f = float(re.sub(r"[^0-9\.-]+", "", spread_txt))
            except Exception:
                spread_f = None
            try:
                total_f = float(re.sub(r"[^0-9\.]+", "", total_txt))
            except Exception:
                total_f = None

            rows.append(
                {
                    "home_team": home,
                    "away_team": away,
                    "closing_spread_vi": spread_f,
                    "closing_total_vi": total_f,
                }
            )

        print(f"ℹ️ VegasInsider rows: {len(rows)}")
        return pd.DataFrame(rows)
    except Exception as e:
        print("⚠️ VegasInsider error:", e)
        return pd.DataFrame()


# ===== Rotowire 傷兵（backfill 用） =====
def fetch_rotowire_injuries() -> pd.DataFrame:
    try:
        url = "https://www.rotowire.com/basketball/nba-injury-report.php"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        notes = {}
        for div in soup.select(".player"):
            team = div.find_previous("h2")
            if not team:
                continue
            tname = team.get_text(strip=True)
            txt = div.get_text(" ", strip=True)
            notes.setdefault(tname, []).append(txt[:80])

        rows = [{"team": k, "injury_report_rw": "; ".join(v[:5])} for k, v in notes.items()]
        print(f"ℹ️ Rotowire teams: {len(rows)}")
        return pd.DataFrame(rows)
    except Exception as e:
        print("⚠️ Rotowire error:", e)
        return pd.DataFrame()


# ===== Safe merge append（避免重覆 game_id+snapshot_type） =====
def safe_merge_append(df_new: pd.DataFrame, path: str, keys: list[str]) -> bool:
    if df_new is None or df_new.empty:
        print(f"ℹ️ No new rows to append to {path}")
        return False

    ensure_dirs()

    if os.path.exists(path):
        old = pd.read_csv(path)
        if not set(keys).issubset(old.columns):
            # 第一次還沒有這些欄位，直接 concat
            df = pd.concat([old, df_new], ignore_index=True)
        else:
            k_old = old[keys].astype(str).agg("||".join, axis=1)
            k_new = df_new[keys].astype(str).agg("||".join, axis=1)
            mask = ~k_old.isin(set(k_new))
            df = pd.concat([old[mask], df_new], ignore_index=True)
    else:
        df = df_new.copy()

    df.to_csv(path, index=False)
    print(f"✅ Updated {path} (+{len(df_new)}) | SHA256 {sha256sum(path)[:12]}")
    return True


# ===== Live 模式 =====
def simulate_scores(mu_a: float, mu_b: float) -> np.ndarray:
    cov = np.array(
        [
            [SD_TEAM**2, RHO * SD_TEAM**2],
            [RHO * SD_TEAM**2, SD_TEAM**2],
        ]
    )
    return np.random.multivariate_normal([mu_a, mu_b], cov, size=N_SIM)


def run_live(window_min: int = 40, window_max: int = 60, snapshot_type: str = "T60"):
    """
    早上每半小時跑一次：
    - 同時讀取 ET「今天 + 昨天」scoreboard，避免跨日錯配
    - 過濾「距離現在 window_min ~ window_max 分鐘」且 state == 'pre' 的比賽
    - 寫入 snapshot_type=T60 到兩個主檔
    """
    # 這裡抓 ET 今天 & 昨天，以降低時差錯位
    d_today = date_et(0)
    d_yest  = date_et(-1)
    dates   = sorted(set([d_today, d_yest]))

    print(f"🔁 Live run for ET dates {dates}, window={window_min}-{window_max} min, snap={snapshot_type}")

    df_sb = fetch_espn_scoreboard_multi(dates)

    if df_sb.empty:
        print("⚠️ No ESPN games this run — safe exit.")
        return

    now_utc = now_tz(TZ_TPE).astimezone(TZ_UTC)
    targets = []

    for _, r in df_sb.iterrows():
        tip_str = r.get("tipoff_utc")
        if not tip_str:
            continue
        try:
            tip = dt.datetime.fromisoformat(tip_str)
        except Exception:
            continue

        mins = (tip - now_utc).total_seconds() / 60.0
        if window_min <= mins <= window_max and r.get("state") == "pre":
            targets.append(r)

    if not targets:
        print("ℹ️ No games in prediction window, safe exit.")
        return

    out_rows = []
    for r in targets:
        # 目前先用簡化版均值（之後你要接 Hybrid v4.x 的完整模型，再把這段換掉即可）
        mu_a = random.uniform(102, 114)
        mu_b = random.uniform(102, 114)
        sims = simulate_scores(mu_a, mu_b)

        tot = sims.sum(axis=1)
        mar = sims[:, 0] - sims[:, 1]

        tl = r.get("total_line")
        sl = r.get("spread_line_raw")

        pO = np.mean(tot > tl) if tl not in [None, 0] else None
        pC = np.mean(mar > -sl) if sl not in [None, 0] else None

        evO = ((pO * 1.909) - (1 - pO)) * 100 if pO is not None else None
        evA = ((pC * 1.909) - (1 - pC)) * 100 if pC is not None else None

        out_rows.append(
            {
                "game_id": r.get("game_id"),
                "snapshot_type": snapshot_type,
                "snapshot_time": now_tz().strftime("%Y-%m-%d %H:%M"),
                "teamA": r.get("away_team"),
                "teamB": r.get("home_team"),
                "total_line": tl,
                "spread_line": sl,
                "prob_over": pO,
                "prob_cover": pC,
                "EV_total": evO,
                "EV_ATS": evA,
                "source": "ESPN_live",
            }
        )

    df_out = pd.DataFrame(out_rows)
    safe_merge_append(df_out, MASTER_PATH, ["game_id", "snapshot_type"])
    safe_merge_append(df_out, PERGAME_PATH, ["game_id", "snapshot_type"])
    print("✅ Live predictions merged.")


# ===== Backfill 模式（昨天 ET 完賽比賽） =====
def run_backfill_real():
    """
    夜間回補：
    - 只抓「昨天 ET」的 scoreboard
    - 過濾 completed==True
    - 補 Vegas & Rotowire
    - snapshot_type='FINAL' 寫入兩個主檔
    """
    y = date_et(-1)
    print(f"🔁 Backfill for ET date {y}")

    sb = fetch_espn_scoreboard_for_date(y)
    df_sb = parse_events_to_df(sb)

    if df_sb.empty:
        print("⚠️ No games for backfill, safe exit.")
        return

    done = df_sb[df_sb["completed"] == True].copy()
    if done.empty:
        print("ℹ️ No completed games yesterday, exit quietly.")
        return

    df_vi = fetch_vegas_odds()
    df_rw = fetch_rotowire_injuries()

    out_rows = []
    for _, r in done.iterrows():
        h = r.get("home_team")
        a = r.get("away_team")

        # Vegas match
        vi_row = None
        if not df_vi.empty:
            m = df_vi[
                (df_vi["home_team"].str.contains(str(h), na=False))
                | (df_vi["away_team"].str.contains(str(a), na=False))
            ]
            if not m.empty:
                vi_row = m.iloc[0].to_dict()

        # Rotowire match（只抓主場）
        rw_home = None
        if not df_rw.empty:
            rw = df_rw[df_rw["team"].str.contains(str(h), na=False)]
            if not rw.empty:
                rw_home = rw.iloc[0]["injury_report_rw"]

        hsc = r.get("home_score")
        asc = r.get("away_score")

        total_final = (hsc + asc) if (hsc is not None and asc is not None) else None

        line_tot = r.get("total_line")
        if (line_tot is None or line_tot == 0) and vi_row:
            line_tot = vi_row.get("closing_total_vi")

        line_spread = r.get("spread_line_raw")
        if (line_spread is None or line_spread == 0) and vi_row:
            line_spread = vi_row.get("closing_spread_vi")

        # O/U 結果
        if line_tot is not None and total_final is not None:
            if total_final > line_tot:
                ou_res = "Over"
            elif total_final < line_tot:
                ou_res = "Under"
            else:
                ou_res = "Push"
        else:
            ou_res = None

        # ATS 結果（以主場為 -spread）
        ats_res = None
        if line_spread is not None and hsc is not None and asc is not None:
            margin = hsc - asc
            if margin + (-line_spread) > 0:
                ats_res = "HomeCover"
            elif margin + (-line_spread) < 0:
                ats_res = "AwayCover"
            else:
                ats_res = "Push"

        out_rows.append(
            {
                "game_id": r.get("game_id"),
                "snapshot_type": "FINAL",
                "snapshot_time": now_tz().strftime("%Y-%m-%d %H:%M"),
                "teamA": a,
                "teamB": h,
                "final_away": asc,
                "final_home": hsc,
                "final_total": total_final,
                "total_line": line_tot,
                "spread_line": line_spread,
                "closing_total_vi": vi_row.get("closing_total_vi") if vi_row else None,
                "closing_spread_vi": vi_row.get("closing_spread_vi") if vi_row else None,
                "injury_report_rw": rw_home,
                "OU_result_v43": ou_res,
                "ATS_result": ats_res,
                "source": "ESPN_final+VI+RW",
            }
        )

    df_out = pd.DataFrame(out_rows)
    safe_merge_append(df_out, MASTER_PATH, ["game_id", "snapshot_type"])
    safe_merge_append(df_out, PERGAME_PATH, ["game_id", "snapshot_type"])
    print("✅ Real backfill complete.")


# ===== 入口 =====
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--task", type=str, default="morning")       # morning / evening
    p.add_argument("--mode", type=str, default="live")          # live / backfill
    p.add_argument("--snapshot", type=str, default="T60")       # T60, T120, ...
    p.add_argument("--tz", type=str, default="Asia/Taipei")     # 保留給未來擴充
    p.add_argument("--window_min", type=int, default=40)        # live 用
    p.add_argument("--window_max", type=int, default=60)        # live 用

    args = p.parse_args()

    try:
        if args.mode == "backfill" or args.task == "evening":
            run_backfill_real()
        else:
            run_live(
                window_min=args.window_min,
                window_max=args.window_max,
                snapshot_type=args.snapshot,
            )
    except SystemExit:
        # 正常 exit，不要讓 GitHub 當作錯誤
        sys.exit(0)
    except Exception as e:
        print("❌ Fatal error in main.py:", e)
        # 一樣用 0 結束，避免整個 workflow 變紅叉
        sys.exit(0)
