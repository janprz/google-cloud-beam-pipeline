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

load_dotenv()

app_name = 'dash-googlebigquerydataplot'

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = 'Dash application'

app.layout = html.Div(
    [
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='interval-component',
            interval=5*1000,
            n_intervals=0
        ),
    ]
)

@app.callback(Output('live-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_plot(n):
    sql = "SELECT * FROM `adzd-project-302811.adzd_twitter_dataset.adzd_twitter_table` LIMIT 1000"

    df = pandas.read_gbq(sql, dialect='standard', use_bqstorage_api=False,project_id=os.getenv('GC_PROJECT'))

    df = df.set_index('time_stamp')
    df = df.sort_index()
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.append_trace({
        'x': df.index,
        'y': df.n_followers,
        'name': 'Tweets',
        'mode': 'lines+markers',
        'type': 'scatter'
    }, 1, 1)
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)




