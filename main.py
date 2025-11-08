# main.py — Hybrid v4.3 自動預測與回抓（修正版）
# by ChatGPT x rex8858

import os
import pandas as pd
import datetime as dt

# ---------- Helper Functions ----------
def safe_read_csv(path):
    """安全讀取 CSV，支援多種編碼"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ 找不到檔案：{path}")

    encodings = ["utf-8", "utf-8-sig", "cp950"]
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            if df.shape[1] == 0:
                raise pd.errors.EmptyDataError
            print(f"✅ 成功以 {enc} 編碼讀取：{path}")
            return df
        except pd.errors.EmptyDataError:
            continue
        except Exception as e:
            print(f"⚠️ 以 {enc} 讀取失敗：{e}")
    raise ValueError(f"❌ 檔案內容為空或格式錯誤：{path}")

# ---------- Paths ----------
master_file = "data/NBA_AB_1030_1107_master_full_v43_TMC_with_summary.csv"
pergame_file = "data/AB_per_game_1030_1107_v43_TMC.csv"

# ---------- Read Data ----------
df_master = safe_read_csv(master_file)
df_pergame = safe_read_csv(pergame_file)

# ---------- 模擬主要處理邏輯（範例） ----------
now = dt.datetime.now()
print(f"\n🕒 開始執行 Hybrid v4.3 自動預測回抓任務：{now.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"主檔案筆數：{len(df_master)}, 每場資料筆數：{len(df_pergame)}")

# 模擬預測邏輯（之後可替換 Hybrid v4.x 核心）
df_master["run_timestamp"] = now
df_pergame["run_timestamp"] = now

# ---------- 儲存輸出 ----------
os.makedirs("logs", exist_ok=True)
df_master.to_csv("logs/NBA_master_updated.csv", index=False, encoding="utf-8-sig")
df_pergame.to_csv("logs/AB_per_game_updated.csv", index=False, encoding="utf-8-sig")
print("✅ 預測與回抓執行完成，結果已輸出至 logs/ 資料夾。")
