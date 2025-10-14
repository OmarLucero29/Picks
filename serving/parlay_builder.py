import pandas as pd
from itertools import combinations
import json, os

PREFS=json.load(open('serving/prefs.json','r',encoding='utf-8'))

def safe_load_picks(path='reports/picks.csv'):
    if not os.path.exists(path) or os.path.getsize(path)==0:
        return pd.DataFrame(columns=['date','sport','league','game','market','selection','line','prob','prob_decimal_odds','confidence','rationale'])
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=['date','sport','league','game','market','selection','line','prob','prob_decimal_odds','confidence','rationale'])

picks=safe_load_picks().drop_duplicates(subset=['game'])
if picks.empty:
    pd.DataFrame(columns=['date','sport','league','game','market','selection','line','prob','prob_decimal_odds','confidence','rationale','parlay_prob','parlay_decimal_odds','note']).to_csv('reports/parlay.csv', index=False)
    print('parlay ok – 0 (sin picks)'); raise SystemExit(0)

picks=picks.sort_values('prob', ascending=False)
target_odds=PREFS.get('parlay_min_combined_odds',2.5); min_prob=PREFS.get('parlay_min_combined_prob',0.55)
best=None
for k in [5,4,3,2]:
    if len(picks)<k: continue
    for combo in combinations(picks.index, k):
        sub=picks.loc[list(combo)].copy()
        p_joint=sub['prob'].prod(); dec_odds=(1.0/sub['prob']).prod()
        if dec_odds>=target_odds and p_joint>=min_prob:
            sub['parlay_prob']=p_joint; sub['parlay_decimal_odds']=dec_odds; best=sub; break
    if best is not None: break
if best is None and len(picks)>=2:
    sub=picks.head(2).copy(); sub['parlay_prob']=sub['prob'].prod(); sub['parlay_decimal_odds']=(1.0/sub['prob']).prod(); best=sub
(best if best is not None else picks.head(0)).assign(note='Parlay generado').to_csv('reports/parlay.csv', index=False)
print('parlay ok')
