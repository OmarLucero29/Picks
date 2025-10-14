# models/predict.py
import json
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

PROC=Path('data/processed')
STORE=Path('models_store')/'active_model.json'

def implied_from_decimal(odds):
    try:
        o=float(odds); 
        return 1.0/o if o>1.0 else None
    except: return None

def load_model():
    if STORE.exists():
        return json.load(open(STORE,'r',encoding='utf-8'))
    return {}

def calibrate_prob(p, sport, row, model):
    alpha=0.15
    if sport=="americano" and "americano" in model and "NFL" in model["americano"]:
        base=model["americano"]["NFL"].get("home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    if sport=="tenis" and "tenis" in model:
        surf=(row.get("surface") or "Unknown")
        base=model["tenis"].get("by_surface",{}).get(surf)
        if base: return alpha*base + (1-alpha)*p
    if sport=="futbol" and "futbol" in model:
        base=model["futbol"].get("by_league",{}).get(row.get("league"))
        if not base: base=model["futbol"].get("global_home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    if sport=="baloncesto" and "baloncesto" in model:
        base=model["baloncesto"].get("NBA",{}).get("home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    if sport=="beisbol" and "beisbol" in model:
        base=model["beisbol"].get("MLB",{}).get("home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    if sport=="hockey" and "hockey" in model:
        base=model["hockey"].get("NHL",{}).get("home_win_rate")
        if base: return alpha*base + (1-alpha)*p
    return p

def main():
    df=pd.read_csv(PROC/'upcoming_events.csv')
    now=datetime.now(timezone.utc)
    df['start_time_utc']=pd.to_datetime(df['start_time_utc'], utc=True, errors='coerce')
    df=df[(df['start_time_utc']>now)]
    model=load_model()
    rows=[]
    for _,r in df.iterrows():
        ph = implied_from_decimal(r.get("ml_home"))
        pa = implied_from_decimal(r.get("ml_away"))
        if ph and pa:
            s=ph+pa; phn=(ph/s)*0.97+0.015; pan=(pa/s)*0.97+0.015
            if phn>=pan:
                p=calibrate_prob(phn, r["sport"], r, model)
                winner=r["home"]; p_win=p; fav="home"
            else:
                p=calibrate_prob(pan, r["sport"], r, model)
                winner=r["away"]; p_win=p; fav="away"
        else:
            defaults={"americano":0.54,"futbol":0.52,"baloncesto":0.54,"beisbol":0.53,"hockey":0.53,"tenis":0.52,"esports":0.52,"ping_pong":0.52}
            p=defaults.get(r["sport"],0.52); winner=r["home"] if p>=0.5 else r["away"]; p_win=p; fav="home" if p>=0.5 else "away"
        total = r.get("market_total") if r.get("market_total")==r.get("market_total") else None
        ou_pick="No Bet"; delta_total=0.0
        spread_line = r.get("spread_line") if r.get("spread_line")==r.get("spread_line") else None
        spread_pick="No Bet"
        if spread_line is not None:
            spread_pick = f"{r['home']} {float(spread_line):+}" if fav=='home' else f"{r['away']} {(-float(spread_line)):+}"
        rows.append(dict(
            date=r.start_time_utc.date().isoformat(), sport=r.sport, league=r.league,
            game=f"{r.home} vs {r.away}", winner=winner, p_win=float(p_win),
            total=(float(total) if total else None), ou_pick=ou_pick, delta_total=float(delta_total),
            spread=spread_pick
        ))
    cols=['date','sport','league','game','winner','p_win','total','ou_pick','delta_total','spread']
    out = pd.DataFrame(rows, columns=cols)  # << asegura encabezados aunque no haya filas
    out.to_csv(PROC/'predictions.csv', index=False)
    print(f"predictions ok – {len(out)} rows -> {PROC/'predictions.csv'}")

if __name__=='__main__': main()
