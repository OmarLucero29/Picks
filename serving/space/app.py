import gradio as gr, pandas as pd, time

def load(path):
    try: return pd.read_csv(path)
    except: return pd.DataFrame()

def filter_sport(df, s):
    if s=="todos" or df.empty: return df
    return df[df["sport"]==s]

with gr.Blocks() as demo:
    gr.Markdown("# Multisport – Top-5 + Parlay 'segurito'")
    gr.Markdown(f"**Última actualización:** {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}")
    sport = gr.Dropdown(choices=['todos','futbol','baloncesto','beisbol','tenis','hockey','ping_pong','americano','esports'], value='todos', label='Filtrar por deporte')
    with gr.Tabs():
        with gr.Tab("Top-5 del día"):
            picks_df = gr.State(load('picks.csv'))
            table = gr.Dataframe(value=filter_sport(load('picks.csv'), 'todos'), interactive=False, wrap=True, height=500)
            dl = gr.File(label="Descargar picks.csv", value='picks.csv', visible=True)
            def update(s):
                df = load('picks.csv')
                return filter_sport(df, s)
            sport.change(fn=update, inputs=sport, outputs=table)
        with gr.Tab("Parlay 'segurito'"):
            gr.Dataframe(load('parlay.csv'), interactive=False, wrap=True, height=400)
            gr.File(label="Descargar parlay.csv", value='parlay.csv', visible=True)

if __name__=='__main__': demo.launch()
