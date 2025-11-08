# NBA Auto (Hybrid v4.3)

這是 Hybrid v4.3（正式版）的自動任務骨架，部署在 GitHub Actions：
- 07:30（台北時間）→ 賽前 T-60 快照與模型預測（可擴充 T-120）
- 19:00（台北時間）→ 昨日（ET）回抓、重算 TOTALS 與滾動統計

## 使用方式
1. 上傳本專案到 GitHub 倉庫
2. 進入 Actions 分頁，啟用 workflow
3. 之後每天自動執行；輸出 CSV 在 `data/`，並隨工作流程上傳 Artifact

> 注意：`src/sources/` 為資料來源解析骨架，實戰需補上各站的解析程式。
