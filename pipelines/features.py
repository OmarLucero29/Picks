from pathlib import Path
import pandas as pd
PROC=Path('data/processed'); PROC.mkdir(parents=True, exist_ok=True)
df=pd.read_csv(PROC/'upcoming_events.csv')
# (placeholder) aquí se pueden crear features por deporte/mercado
df.to_csv(PROC/'upcoming_events_features.csv', index=False)
print('features ok –', len(df))
