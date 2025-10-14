# Multisport Starter – FINAL

**Incluye** todos los deportes y mercados en estructura, datos live (TheOddsAPI/PandaScore), históricos (NFL/tenis/NBA/MLB/NHL + fútbol incremental), modelos (odds + calibración) y publicación a Hugging Face Space. Notificación opcional a Telegram.

## Secrets requeridos
- `HF_TOKEN`, `HF_SPACE`
- `ODDS_API_KEY`
- `APIFOOTBALL_KEY` (fútbol histórico incremental)
- `PANDASCORE_TOKEN` (e-sports, opcional)
- (Opcional) `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

## Flujos
- **daily**: fetch → features → predict → Top-5 → Parlay → Space (+ Telegram)
- **weekly**: históricos → train → backtest → predict → Top-5 → Parlay → Space (+ Telegram)

> Los props y mercados secundarios están definidos en `config/markets.yaml`. El sistema publica ML/Spread/OU cuando hay líneas; si falta información, marca `No Bet`.
