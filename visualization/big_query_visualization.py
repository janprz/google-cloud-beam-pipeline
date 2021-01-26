from google.cloud import bigquery
from dotenv import load_dotenv
import os
import pandas
import matplotlib

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
import plotly

from dash.dependencies import Output, Input
from io import BytesIO
from wordcloud import WordCloud
import base64
from collections import Counter

load_dotenv()

app_name = 'dash-googlebigquerydataplot'

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = 'Dash application'

app.layout = html.Div(
    [
        dcc.Graph(id='live-graph', animate=True, style={'height': '90vh'}),
        dcc.Graph(id='batch-graph', animate=True, style={'height': '90vh'}),
        dcc.Interval(
            id='interval-component',
            interval=5*1000,
            n_intervals=0
        ),
        html.Img(id="image_wc")
    ]
)

@app.callback(Output('live-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_plot(n):
    sql = "SELECT * FROM `adzd-project-302811.adzd_twitter_dataset.adzd_twitter_table` ORDER BY time_stamp DESC LIMIT 10000"

    df = pandas.read_gbq(sql, dialect='standard', use_bqstorage_api=False,project_id=os.getenv('GC_PROJECT'))

    df = df.set_index('time_stamp')
    df = df.sort_index()
    fig = plotly.subplots.make_subplots(rows=2, cols=1)
    fig.append_trace({
        'x': df.index,
        'y': df.n_followers,
        'name': 'Followers',
        'mode': 'markers',
        'type': 'scatter',
        'text': df.text
    }, 1, 1)
    fig.append_trace({
        'x': df.index,
        'y': df.sentiment_score,
        'name': 'Sentiments',
        'mode': 'markers',
        'type': 'scatter',
        'text': df.text
    }, 2, 1)
    return fig

@app.callback(Output('batch-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_plot(n):
    sql = "SELECT * FROM `adzd-project-302811.adzd_twitter_dataset.adzd_twitter_table_batch` ORDER BY time_stamp DESC LIMIT 10000"

    df = pandas.read_gbq(sql, dialect='standard', use_bqstorage_api=False,project_id=os.getenv('GC_PROJECT'))

    df = df.set_index('time_stamp')
    df = df.sort_index()
    fig = plotly.subplots.make_subplots(rows=2, cols=1)
    fig.append_trace({
        'x': df.index,
        'y': df.avg_num_words,
        'name': 'Avg number of words'
    }, 1, 1)
    fig.append_trace({
        'x': df.index,
        'y': df.avg_num_characters,
        'name': 'Avg number of characters'
    }, 2, 1)
    return fig

def plot_wordcloud(data):
    wc = WordCloud(background_color='black', width=800, height=600)
    wc.fit_words(data)
    return wc.to_image()

@app.callback(Output('image_wc', 'src'), Input('image_wc', 'id'))
def update_wc(n):
    sql = "SELECT * FROM `adzd-project-302811.adzd_twitter_dataset.adzd_twitter_table_batch` ORDER BY time_stamp DESC LIMIT 10"

    df = pandas.read_gbq(sql, dialect='standard', use_bqstorage_api=False,project_id=os.getenv('GC_PROJECT'))

    df = df.set_index('time_stamp')
    df = df.sort_index()

    cnt = Counter()
    [cnt.update(Counter(top)) for top in df.top_words.str.split(',')]
    img = BytesIO()
    plot_wordcloud(data={x:y for x,y in cnt.most_common(40)}).save(img, format='PNG')
    return 'data:image/png;base64,{}'.format(base64.b64encode(img.getvalue()).decode())


if __name__ == '__main__':
    app.run_server(debug=True)
