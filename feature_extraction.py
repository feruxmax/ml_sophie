import pandas as pd
import numpy as np
from datetime import datetime

def to_platform_indep(data, platform):
    if platform=='android':
        data.drop(['ios_ifa', 'ios_ifv', 'google_aid'], axis=1, inplace=True)

        # rename cols
        names = data.columns.tolist()
        names[names.index('android_id')] = 'id'
        data.columns = names
    elif platform=='ios':
        data.drop(['android_id','google_aid', 'ios_ifa'], axis=1, inplace=True)

        # rename cols
        names = data.columns.tolist()
        names[names.index('ios_ifv')] = 'id'
        data.columns = names
    return data
    
def timestamp_mod(timestamp, period):
    return (timestamp//period)*period
    
###############################################################################
FILENAME = 'events_android.csv'
PLATFORM = 'android'
PERIOD = 24*3600 # 24h

data = pd.read_csv(FILENAME)
data.sort_values(by='event_datetime', inplace=True)
data.reset_index(drop=True, inplace=True)

data = to_platform_indep(data, PLATFORM)

cols = ['event_datetime', 'id']
cols.extend(data['event_name'].unique().tolist())
features = pd.DataFrame(columns = cols, index = [])

first_timestamp = timestamp_mod(data['event_timestamp'].iloc[0], PERIOD)
end_timestamp = data['event_timestamp'].iloc[-1]

#%% 
for period_starts in np.arange(first_timestamp, end_timestamp+1, PERIOD):
    cur_features = pd.DataFrame(columns = cols, index = [])
    cur_period_data = data[(period_starts <= data['event_timestamp']) &
                           (data['event_timestamp'] < period_starts + PERIOD)]
    periods_ids = cur_period_data['id'].unique()

    for id in periods_ids:
        ids_data4period = cur_period_data[cur_period_data['id'] == id]
        events = ids_data4period['event_name'].value_counts()   
        cur_features.loc[len(cur_features)] = events
    
    if len(cur_features) > 0 :
        date_str = cur_period_data['event_datetime'].iloc[0].split(' ')[0]
        date = datetime.strptime(date_str,'%Y-%m-%d')
        cur_features['id'] = periods_ids                
        cur_features['event_datetime'] = date

    features = features.append(cur_features, ignore_index=True)

features.fillna(0, inplace=True)
features = features.set_index(['event_datetime','id'])
features.to_csv("1.csv", sep=',', header=True) 