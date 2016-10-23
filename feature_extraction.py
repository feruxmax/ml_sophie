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
    
###############################################################################
FILENAME = 'events_android.csv'
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
    cur_features = pd.DataFrame(columns = cols, index = [])
    cur_period_data = data[(period_starts <= data['event_datetime']) &
                           (data['event_datetime'] < period_starts + PERIOD)]
    periods_ids = cur_period_data['id'].unique()
    
    # cycle over ids in period
    for id in periods_ids:
        ids_data4period = cur_period_data[cur_period_data['id'] == id]
        events = ids_data4period['event_name'].value_counts()   
        cur_features.loc[len(cur_features)] = events
    
    if len(cur_features) > 0 :
        cur_features['id'] = periods_ids                
        cur_features['event_datetime'] = \
                round_datetime(cur_period_data['event_datetime'].iloc[0], PERIOD)

    features = features.append(cur_features, ignore_index=True)

# final clean
features.fillna(0, inplace=True)
features = features.set_index(['event_datetime','id'])
features.to_csv("1.csv", sep=',', header=True) 