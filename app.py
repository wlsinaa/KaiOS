import os
import dash
from dash import dcc
from dash import html
from pyspark.sql import SparkSession
import plotly.express as px
from plotly import graph_objects as go
import pandas as pd
from datetime import date
from analyzeEngine import DMAU
from dataEngine import dataimport

app = dash.Dash()

def getdataset(datasetname:str):
    test_ingress = dataimport.ingress()
    test_ingress.setDataPath(f'src\\data\\{datasetname}.csv') # C:\\Users\\ivywa\\OneDrive\\Desktop\\portfolio\\kaiOS\\src\\data\\
    test_ingress.load()
    test_ingress.colToDate('server_time', 'MM/dd/yy HH:mm')
    dataset = test_ingress.getdata()
    classifier  = DMAU.classify(dataset)
    classifier.setUserCol("device_id")
    # classifier.getNewUsers("server_time",1).show()
    # classifier.getCertainUSER("server_time",datetime(2018,12,1),datetime(2018,12,9)).show()
    base_date = date(2018,12,31)
    classifier.addRollingCols('roll_days','server_time',base_date, "reverse", 1,365)
    classifier.addRollingCols('roll_weeks','server_time',base_date, "reverse", 7,52)
    test_day_df = classifier.getDailyUsers('roll_weeks',7,4)
    test_week_df = classifier.getWeeklyUsers('roll_weeks',4,3)
    pd_test_day_df = test_day_df
    pd_test_week_df = test_week_df
    return pd_test_day_df,pd_test_week_df

def save_funnel_csv(datasetname:str ): # 'ads_stream'
    day,week = getdataset(datasetname)
    day.filter(~(day['event_type'] == 'close') & ~(day['event_type'] == 'logs')).toPandas().to_csv(f'src\\data\\day_{datasetname}_funnel.csv',index=False) # filter "close"
    week.filter(~(week['event_type'] == 'close') & ~(week['event_type'] == 'logs')).toPandas().to_csv(f'src\\data\\week_{datasetname}_funnel.csv',index=False) # filter "close"

for datasetname in ['ads_stream','store_stream']:
    if not os.path.exists(f'src\\data\\day_{datasetname}_funnel.csv'):
        save_funnel_csv(datasetname)
    if not os.path.exists(f'src\\data\\week_{datasetname}_funnel.csv'):
        save_funnel_csv(datasetname)

spark = SparkSession.builder.appName("default").getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def get_funnel_adsdata(item_name:str,by, day_or_week:str):
    df = spark.read.csv(f'src\\data\\{day_or_week}_ads_stream_funnel.csv',header=True)
    load_factor = df.filter((df['event_type'] == 'load') & (df[ by] == item_name)).count()
    display_factor = df.filter((df['event_type'] == 'display') & (df[by] == item_name)).count()
    click_factor = df.filter((df['event_type'] == 'click') & (df[ by] == item_name)).count()
    return load_factor,display_factor,click_factor

def get_funnel_storedata(item_name:str,by, day_or_week:str):
    df = spark.read.csv(f'src\\data\\{day_or_week}_store_stream_funnel.csv',header=True)
    open_factor = df.filter((df['event_type'] == 'open') & (df[ by] == item_name)).count()
    view_factor = df.filter((df['event_type'] == 'app_view') & (df[by] == item_name)).count()
    down_factor = df.filter((df['event_type'] == 'download') & (df[ by] == item_name)).count()

    return open_factor,view_factor,down_factor
target_cols = ["load", "display", "click"]
target_cols_store = ['open','app_view','download']
fig = go.Figure()
# get Funnel data
china_load_factor,china_display_factor,china_click_factor = get_funnel_adsdata('China','country','day')
India_load_factor,India_display_factor,India_click_factor = get_funnel_adsdata('India','country','day')
Nigeria_load_factor,Nigeria_display_factor,Nigeria_click_factor = get_funnel_adsdata('Nigeria','country','day')
US_load_factor,US_display_factor,US_click_factor = get_funnel_adsdata('United States','country','day')

fig.add_trace(go.Funnel(
    name = 'China',
    y = target_cols,
    x = [china_load_factor,china_display_factor,china_click_factor],
    textinfo = "value+percent initial"))

fig.add_trace(go.Funnel(
    name = 'India',
    orientation = "h",
    y = target_cols,
    x = [India_load_factor,India_display_factor,India_click_factor],
    textposition = "inside",
    textinfo = "value+percent previous"))

fig.add_trace(go.Funnel(
    name = 'Nigeria',
    orientation = "h",
    y = target_cols,
    x = [Nigeria_load_factor,Nigeria_display_factor,Nigeria_click_factor],
    textposition = "inside",
    textinfo = "value+percent previous"))

fig.add_trace(go.Funnel(
    name = 'United States',
    orientation = "h",
    y = target_cols,
    x = [US_load_factor,US_display_factor,US_click_factor],
    textposition = "inside",
    textinfo = "value+percent previous"))

china_load_factor,china_display_factor,china_click_factor = get_funnel_adsdata('China','country','week')
India_load_factor,India_display_factor,India_click_factor = get_funnel_adsdata('India','country','week')
Nigeria_load_factor,Nigeria_display_factor,Nigeria_click_factor = get_funnel_adsdata('Nigeria','country','week')
US_load_factor,US_display_factor,US_click_factor = get_funnel_adsdata('United States','country','week')
fig2 = go.Figure()

fig2.add_trace(go.Funnel(
    name = 'China',
    y = target_cols,
    x = [china_load_factor,china_display_factor,china_click_factor],
    textinfo = "value+percent initial"))

fig2.add_trace(go.Funnel(
    name = 'India',
    orientation = "h",
    y = target_cols,
    x = [India_load_factor,India_display_factor,India_click_factor],
    textposition = "inside",
    textinfo = "value+percent previous"))

fig2.add_trace(go.Funnel(
    name = 'Nigeria',
    orientation = "h",
    y = target_cols,
    x = [Nigeria_load_factor,Nigeria_display_factor,Nigeria_click_factor],
    textposition = "inside",
    textinfo = "value+percent previous"))

fig2.add_trace(go.Funnel(
    name = 'United States',
    orientation = "h",
    y = target_cols,
    x = [US_load_factor,US_display_factor,US_click_factor],
    textposition = "inside",
    textinfo = "value+percent previous"))


brands = ["A", "B", "C", "D", "E"]
def plot(figure,brand: str,day_or_week:str):
    load_factor,display_factor,click_factor = get_funnel_adsdata("Brand "+ brand,'brand',day_or_week)
    figure.add_trace(go.Funnel(
    name = "Brand "+ brand,
    y = target_cols,
    x = [load_factor,display_factor,click_factor],
    textinfo = "value+percent initial"))
    return figure

fig3 = go.Figure()
for brand in brands:
    fig3 = plot(fig3,brand,'day')

fig4 = go.Figure()
for brand in brands:
    fig4 = plot(fig4,brand,'week')
# 

def plot(figure,brand: str,day_or_week:str):
    open_factor,view_factor,down_factor = get_funnel_storedata("Brand "+ brand,'brand',day_or_week)
    figure.add_trace(go.Funnel(
    name = "Brand "+ brand,
    y = target_cols_store,
    x = [open_factor,view_factor,down_factor],
    textinfo = "value+percent initial"))
    return figure

fig5 = go.Figure()
for brand in brands:
    fig5 = plot(fig5,brand,'day')



app.layout = html.Div(children=[html.Div([
        html.H1(children='(Daily AU) (Advertisement) by country, load-> display->click'),
        dcc.Graph(id="life-exp-vs-gdp", figure=fig)
        ]),
        html.Div([

        html.H1(children='(Weekly AU) (Advertisement) by country, load-> display->click'),
        dcc.Graph(id="life-exp-vs-gdp2", figure=fig2)

        ]),
        html.Div([

        html.H1(children='(Daily AU) (Advertisement) by brand, load-> display->click'),
        dcc.Graph(id="life-exp-vs-gdp3", figure=fig3)

        ]),
        html.Div([

        html.H1(children='(Weekly AU) (Advertisement) by brand, load-> display->click'),
        dcc.Graph(id="life-exp-vs-gdp4", figure=fig4)

        ]),

        html.Div([

        html.H1(children='(Daily AU) (Store) by brand, open-> view->download'),
        dcc.Graph(id="life-exp-vs-gdp5", figure=fig5)

        ])

        
    ])


if __name__ == "__main__":
    app.run_server(debug=True)