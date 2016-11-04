import pandas as pd
import numpy as np
import datetime as dt

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

def prepare_data(data):
    data.sort_values(by='event_datetime', inplace=True)
    data.reset_index(drop=True, inplace=True)
    data['event_datetime'] = pd.to_datetime(data['event_datetime'], format='%Y-%m-%d %H:%M:%S')
    return to_platform_indep(data, PLATFORM)
    
def round_datetime(date_time, date_delta):
    rounded = None
    if(date_delta.days > 0):
        rounded = date_time - dt.timedelta(days = date_time.day%date_delta.days)
        rounded = rounded.replace(hour = 0, minute = 0, second=0)
    else:
        rounded = date_time - dt.timedelta(hours = date_time.hour%(date_delta.seconds/3600))
        rounded = rounded.replace(minute = 0, second=0)

    return rounded

def features_on_interval(data, start, interval):
    features = pd.DataFrame(columns = cols, index = [])
    period_data = data[(start <= data['event_datetime']) &
                       (data['event_datetime'] < start + interval)]
    periods_ids = period_data['id'].unique()
    
    for id in periods_ids:
        ids_data4period = period_data[period_data['id'] == id]
        events = ids_data4period['event_name'].value_counts()   
        features.loc[len(features)] = events

    if len(features) > 0 :
        features['id'] = periods_ids                
        features['event_datetime'] = \
                round_datetime(period_data['event_datetime'].iloc[0], interval)
    return features
#%%     
###############################################################################
FILENAME = 'data/events_android.csv'
OUTFILE = 'data/android.csv'
PLATFORM = 'android'
PERIOD = dt.timedelta(days=1)

# input data
data = pd.read_csv(FILENAME)
data = prepare_data(data)

# for output data
cols = ['event_datetime', 'id']
cols.extend(data['event_name'].unique().tolist())
features = pd.DataFrame(columns = cols, index = [])

# cycle over date-time periods
first_date = round_datetime(data['event_datetime'].iloc[0], PERIOD)
end_date = round_datetime(data['event_datetime'].iloc[-1], PERIOD)
date_list = [first_date + x*PERIOD for x in range(0, (end_date-first_date+PERIOD)//PERIOD)]
#%% 
for period_starts in date_list:
    cur_features = features_on_interval(data, period_starts, PERIOD)
    features = features.append(cur_features, ignore_index=True)

# final clean
features.fillna(0, inplace=True)
features = features.set_index(['event_datetime','id'])
features.to_csv(OUTFILE, sep=',', header=True) 