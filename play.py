import pandas as pd
wt1 = pd.read_csv("WT002.csv")
from bokeh.charts import Scatter, output_file, show

data = dict(active_power = wt1['WT002_ACTIVE_POWER'], wind_direction=wt1['WT002_WIND_DIRECTION'])
p = Scatter(
  data, x='active_power', y='wind_direction', xlabel='Active Power',
  ylabel='Wind Direction', title='Active Power vs Wind Direction', color='green'
)

output_file("002_active_power.html")
show(p)