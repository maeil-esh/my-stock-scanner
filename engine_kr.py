name: KR-Scan

on:
  schedule:
    - cron: '30 0 * * 1-5'   # 09:30 KST
    - cron: '0 4 * * 1-5'    # 13:00 KST
    - cron: '0 7 * * 1-5'    # 16:00 KST
  workflow_dispatch:

# [FIX] 동시 실행 방지 — 같은 시간대 push 충돌 차단
concurrency:
  group: kr-scan
  cancel-in-progress: false

jobs:
  kr-scan:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    env:
      DART_API_KEY:     ${{ secrets.DART_API_KEY }}
      TELEGRAM_TOKEN:   ${{ secrets.TELEGRAM_TOKEN }}
      TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'          # [FIX] 3.9 → 3.11 (pykrx 호환성)

      # [FIX] pip 캐시 — 매 실행마다 재설치 방지
      - name: Cache pip
        uses: actions/cache@v3
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt') }}
          restore-keys: |
            ${{ runner.os }}-pip-

      - name: Install libraries
        run: pip install -r requirements.txt   # [FIX] 인라인 목록 → requirements.txt

      - name: Run KR Scanner
        run: python engine_kr.py

      # [FIX] git 로직 개선 — stash 루프 제거, diff 체크로 불필요 commit 방지
      - name: Save Results
        run: |
          git config --global user.name 'GitHub Action'
          git config --global user.email 'action@github.com'
          git pull --rebase origin main
          git add stock_data.json history.json 2>/dev/null || true
          git diff --cached --quiet || git commit -m "KR scan $(TZ='Asia/Seoul' date +'%Y-%m-%d %H:%M KST')"
          git push
