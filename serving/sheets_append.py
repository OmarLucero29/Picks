# .github/workflows/append-parlays.yml
name: Append Parlays

on:
  workflow_dispatch:

jobs:
  append-parlays:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      # OPCIÓN A (recomendada): secret en BASE64
      # Crea el secret: GOOGLE_SERVICE_ACCOUNT_JSON_B64 con el JSON en base64
      - name: Write service_account.json from BASE64 secret
        if: env.USE_B64 == '1'
        env:
          GOOGLE_SERVICE_ACCOUNT_JSON_B64: ${{ secrets.GOOGLE_SERVICE_ACCOUNT_JSON_B64 }}
          USE_B64: 1
        run: |
          echo "$GOOGLE_SERVICE_ACCOUNT_JSON_B64" | base64 -d > service_account.json
          python - << 'PY'
import json,sys
j=json.load(open("service_account.json"))
assert "client_email" in j and "private_key" in j, "Credencial inválida"
print("Credencial OK:", j["client_email"])
PY

      # OPCIÓN B: secret como JSON crudo (escapado). Crea GOOGLE_SERVICE_ACCOUNT_JSON
      - name: Write service_account.json from raw JSON secret
        if: env.USE_B64 != '1'
        env:
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_SERVICE_ACCOUNT_JSON }}
        run: |
          cat > service_account.json <<'JSON'
${GOOGLE_SERVICE_ACCOUNT_JSON}
JSON
          python - << 'PY'
import json,sys
j=json.load(open("service_account.json"))
assert "client_email" in j and "private_key" in j, "Credencial inválida"
print("Credencial OK:", j["client_email"])
PY

      - name: Run append script
        env:
          SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}   # <-- agrega este secret con el ID del Sheet
        run: |
          python serving/sheets_append.py
