# models/predict.py
import math
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

PROC=Path('data/processed')

def clamp(x,lo,hi): return max(lo,min(hi,x))
def implied_from_decimal(odds):
    try:
        o=float(odds)
        return 1.0/o if o>1.0 else None
    except: return None

def pred_from_odds(r):
    ph = implied_from_decimal(r.get("ml_home"))
    pa = implied_from_decimal(r.get("ml_away"))
    if ph and pa:
        s = ph + pa
        phn = (ph/s)*0.97 + 0.015
        pan = (pa/s)*0.97 + 0.015
        winner = r["home"] if phn>=pan else r["away"]
        p_win = max(phn, pan)
        return winner, p_win
    return None, None

def pred_generic(r):
    sport = r["sport"]
    defaults = {
        "americano": (0.54, r.get("market_total",44.0)),
        "futbol": (0.52, r.get("market_total",2.5)),
        "baloncesto": (0.54, r.get("market_total",225.0)),
        "beisbol": (0.53, r.get("market_total",8.5)),
        "hockey": (0.53, r.get("market_total",6.0)),
        "tenis": (0.52, r.get("market_total",22.5)),
        "esports": (0.52, None),
        "ping_pong": (0.52, None)
    }
    p_home, total = defaults.get(sport, (0.52, None))
    winner = r["home"] if p_home>=0.5 else r["away"]
    return winner, p_home, total

def main():
    df=pd.read_csv(PROC/'upcoming_events.csv')
    now=datetime.now(timezone.utc)
    df['start_time_utc']=pd.to_datetime(df['start_time_utc'], utc=True, errors='coerce')
    df=df[(df['start_time_utc']>now)]
    rows=[]
    for _,r in df.iterrows():
        w,p = pred_from_odds(r)
        if w is not None:
            rows.append(dict(
                date=r.start_time_utc.date().isoformat(), sport=r.sport, league=r.league,
                game=f"{r.home} vs {r.away}", winner=w, p_win=float(p),
                total=(float(r.get("market_total")) if r.get("market_total")==r.get("market_total") else None),
                ou_pick="No Bet", delta_total=0.0
            ))
        else:
            w, p_home, total = pred_generic(r)
            rows.append(dict(
                date=r.start_time_utc.date().isoformat(), sport=r.sport, league=r.league,
                game=f"{r.home} vs {r.away}", winner=w, p_win=float(p_home),
                total=(float(total) if total is not None else None),
                ou_pick="No Bet", delta_total=0.0
            ))
    out=pd.DataFrame(rows); out.to_csv(PROC/'predictions.csv', index=False)
    print(f"predictions ok – {len(out)} rows -> {out}")

if __name__=='__main__': main()