# connect to google 

from pytrends.request import TrendReq
import numpy as np
import pandas as pd
import sys
import os
import matplotlib.pyplot as plt

sys.path.append(os.path.dirname(os.getcwd()))
from shared.src import bigquery

bq = bigquery.GoogleBigQuery(os.path.join(os.path.dirname(os.getcwd()), "evi-stitch-3e0baed4ba0a.json"))
facebook = bq.obter_dados_facebook()

facebook = facebook[facebook['impressions'] >= 1]

facebook['cpm'] = facebook['spend']/facebook['impressions']*1000
pytrends = TrendReq(hl='pt-BR', tz=180) 



def get_trends(kw_list, name_avg = 'avg'):
        
    pytrends.build_payload(kw_list, cat=0, timeframe='today 5-y', geo = 'BR') 

    #1 Interest over Time
    data = pytrends.interest_over_time() 

    scale_data = data / data.mean()
    scale_data = scale_data.drop('isPartial', axis = 1)
    scale_data[name_avg] = np.mean(scale_data, axis = 1)
    scale_data = scale_data[scale_data.index > '2021-01-01']
    # scale_data.reset_index(inplace=True)
    # scale_data['date'] = pd.to_datetime(scale_data['date']).dt.date
    return scale_data

ecommerce_kw = ["mercado livre", "magazine luiza", "OLX", "shopee", "belezanaweb"] # list of keywords to get data 
ecom_data = get_trends(ecommerce_kw, 'ecom_avg'); ecom_data
# ecom_data.plot()
# plt.show()

beauty_kw = ["boticario", "natura", "avon", "sallve"] # list of keywords to get data 
beauty_data = get_trends(beauty_kw, 'beauty_avg')
# beauty_data.plot()
# plt.show()

moment_kw = ["skincare", "sérum", "creme rosto", "hialurônico"] # list of keywords to get data 
moment_data = get_trends(moment_kw, 'moment_avg')
# moment_data.plot()
# plt.show()


####


fb = facebook.groupby('date_start').agg(
    impressions = pd.NamedAgg(column = 'impressions', aggfunc = sum),
    clicks = pd.NamedAgg(column = 'inline_clicks', aggfunc = sum),
    cost = pd.NamedAgg(column = 'spend', aggfunc = sum),
    cpm_avg = pd.NamedAgg(column = 'cpm', aggfunc = np.mean)
)

fb['cpm_mean'] = fb['cost']/fb['impressions']*1000
fb['cpm_mean_mean'] = fb['cpm_mean']/np.mean(fb['cpm_mean'])
fb['cpm_avg_mean'] = fb['cpm_avg']/np.mean(fb['cpm_avg'])
fb.index = pd.to_datetime(fb.index)
# fb.index.name = 'date'
# fb.reset_index(inplace=True)

# fb2 = fb.merge(ecom_data, on = 'date').merge(beauty_data, on = 'date').merge(moment_data, on = 'date')

# fb2 = fb2.loc[:,['date', 'cpm_mean_mean', 'cpm_avg_mean','avg_x', 'avg_y', 'avg']]

# fb2['avg_of_avgs'] = fb2.loc[:,['avg', 'avg_x','avg_y']].mean(axis = 1)

# fb2['avg_of_avgs'] = fb2['avg']*0.1 + fb2['avg_x']*-0.2 + fb2['avg_y']*0.5
# fb2['avg_of_avgs'] = fb2['avg_of_avgs']/np.mean(fb2['avg_of_avgs'])
# fb2.corr()

# fb2.loc[:,['cpm_mean_mean','avg_of_avgs', 'cpm_avg_mean']].plot()
# plt.show()

def calc_residuals(result):
    residuals = (result['avg_of_avgs']-result['cpm_avg_mean'])
    res = sum((residuals[residuals > 0])**2)
    return(res)

result = pd.concat([fb[fb.index > '2021-01-01'], ecom_data], axis=1)
result = pd.concat([result, beauty_data], axis=1)
result = pd.concat([result, moment_data], axis=1)

result.loc[:,['ecom_avg', 'beauty_avg','moment_avg']] = result.loc[:,['ecom_avg', 'beauty_avg','moment_avg']].interpolate(method='linear')
result['avg_of_avgs'] = result['ecom_avg']*-0.34 + result['beauty_avg']*0.87 + result['moment_avg']*0.21
result['avg_of_avgs'] = result['avg_of_avgs']/np.mean(result['avg_of_avgs'])
# result['avg_of_avgs'] = (result['avg_of_avgs'] - 1)*1 + 1
# result.loc[:,['cpm_mean_mean', 'cpm_avg_mean','ecom_avg', 'beauty_avg','moment_avg', 'avg_of_avgs']].corr()
calc_residuals(result)

result.loc[:,['cpm_avg_mean', 'avg_of_avgs']].plot()
plt.show()


res = pd.DataFrame(columns = ['w1','w2','w3','residuals'])
for x1 in np.arange(-1, 1.25, 0.26):
    for x2 in np.arange(-1, 1.25, 0.26):
        for x3 in np.arange(-1, 1.25, 0.26):
            result.loc[:,['ecom_avg', 'beauty_avg','moment_avg']] = result.loc[:,['ecom_avg', 'beauty_avg','moment_avg']].interpolate(method='linear')
            result['avg_of_avgs'] = result['ecom_avg']*x1 + result['beauty_avg']*x2 + result['moment_avg']*x3
            result['avg_of_avgs'] = result['avg_of_avgs']/np.mean(result['avg_of_avgs'])
            # result['avg_of_avgs'] = (result['avg_of_avgs'] - 1)*1 + 1
            # result.loc[:,['cpm_mean_mean', 'cpm_avg_mean','ecom_avg', 'beauty_avg','moment_avg', 'avg_of_avgs']].corr()
            res = res.append({'w1':x1, 'w2':x2, 'w3':x3, 'residuals':calc_residuals(result)}, ignore_index=True)
            
x1 = res['w3']
y = np.log(res['residuals'])

# create the plot
plt.plot(x1, y, 'o')
plt.show()

res[res['residuals'] == res['residuals'].min()]


residuals = (result['avg_of_avgs']-result['cpm_avg_mean'])

result['res'] = residuals

sum((residuals[residuals > 0])**2)


residuals.isnull()

np.nan
