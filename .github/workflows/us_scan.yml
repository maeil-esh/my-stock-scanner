name: US-Scan

on:
  schedule:
    - cron: '0 22 * * 1-5'   # 07:00 KST
  workflow_dispatch:

jobs:
  us-scan:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    env:
      TELEGRAM_TOKEN:   ${{ secrets.TELEGRAM_TOKEN }}
      TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install libraries
        run: |
          pip install yfinance requests numpy beautifulsoup4

      - name: Run US Scanner
        run: python engine_us.py

      - name: Save Results
        run: |
          git config --global user.name 'GitHub Action'
          git config --global user.email 'action@github.com'
          git pull --rebase origin main || true
          git add stock_data_us.json
          git commit -m "US scan $(date +'%Y-%m-%d %H:%M KST')" || echo "No changes"
          git push
