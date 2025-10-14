import pandas as pd, numpy as np
from pathlib import Path
from pandas.errors import EmptyDataError

PROC=Path('data/processed'); REPORTS=Path('reports'); REPORTS.mkdir(parents=True, exist_ok=True)

def fair_odds(p): p=max(min(p,0.999),0.001); return 1.0/p

def safe_read_preds():
    p = PROC/'predictions.csv'
    if not p.exists() or p.stat().st_size==0:
        return pd.DataFrame(columns=['date','sport','league','game','winner','p_win','total','ou_pick','delta_total','spread'])
    try:
        return pd.read_csv(p)
    except EmptyDataError:
        return pd.DataFrame(columns=['date','sport','league','game','winner','p_win','total','ou_pick','delta_total','spread'])

preds = safe_read_preds()
if preds.empty:
    # escribir archivo de salida vacío pero con columnas correctas
    cols=['date','sport','league','game','market','selection','line','prob','prob_decimal_odds','confidence','rationale']
    pd.DataFrame(columns=cols).to_csv('reports/picks.csv', index=False)
    print('picks ok – 0 (no hay eventos)'); raise SystemExit(0)

preds = preds.sort_values('p_win', ascending=False)

chosen=[]; used_games=set(); used_leagues=set()
for _,r in preds.iterrows():
    if len(chosen)>=5: break
    if r['game'] in used_games: continue
    if r['league'] in used_leagues and len(chosen)<3: 
        continue
    chosen.append(r); used_games.add(r['game']); used_leagues.add(r['league'])

if len(chosen)<5:
    for _,r in preds.iterrows():
        if len(chosen)>=5: break
        if r['game'] in used_games: continue
        chosen.append(r); used_games.add(r['game'])

base=pd.DataFrame(chosen[:5]).copy()
if base.empty:
    cols=['date','sport','league','game','market','selection','line','prob','prob_decimal_odds','confidence','rationale']
    pd.DataFrame(columns=cols).to_csv('reports/picks.csv', index=False)
    print('picks ok – 0'); raise SystemExit(0)

picks=base.rename(columns={'winner':'selection','p_win':'prob'})
picks['market']='ML'; picks['line']=''
picks['prob_decimal_odds']=picks['prob'].apply(fair_odds)
picks['confidence']=np.where(picks['prob']>=0.7,'Alta',np.where(picks['prob']>=0.6,'Media','Baja'))
picks['rationale']="Predicción basada en odds ajustadas y baselines históricos por deporte/competencia."
cols=['date','sport','league','game','market','selection','line','prob','prob_decimal_odds','confidence','rationale']
picks[cols].to_csv('reports/picks.csv', index=False)
print('picks ok –', len(picks))
