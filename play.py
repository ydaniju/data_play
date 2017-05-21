import pandas as pd
wt1 = pd.read_csv("WT002.csv")
from bokeh.charts import Scatter, output_file, show

data = dict(active_power = wt1['WT002_ACTIVE_POWER'], wind_speed=wt1['WT002_TURBINE_STATUS'])
p = Scatter(
  data, y='active_power', x='wind_speed', ylabel='Active Power',
  xlabel='Wind Speed', title='Active Power vs Wind Speed', color='green'
)

output_file("002_active_power.html")
show(p)