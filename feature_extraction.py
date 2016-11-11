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
    
def get_hours(time_delta):
    return 24*time_delta.days + time_delta.seconds//3600

def features_on_interval(data, start, interval):
    features = pd.DataFrame(columns = cols, index = [])
    
    interval_data = data[(start <= data['event_datetime']) &
                       (data['event_datetime'] < start + interval)]
    interval_ids = interval_data['id'].unique()
    
    for id in interval_ids:
        ids_data4interval = interval_data[interval_data['id'] == id]
        events = ids_data4interval['event_name'].value_counts()
        features.loc[len(features)] = events

    if len(features) > 0 :
        features['id'] = interval_ids                
        features['event_datetime'] = \
                round_datetime(interval_data['event_datetime'].iloc[0], interval)
    return features
    
def fill_target(cur_f, next_f):
    if len(cur_f) == 0 or len(next_f) == 0:
        return cur_f
        
    cur_f.set_index('id', inplace=True)
    next_f.set_index('id', inplace=True)
    
    cur_f[TARGET] = next_f[SOURCE_TARGET]
    cur_f.reset_index(level=0, inplace=True)
    return cur_f
    
def sum_features_for(features, i, T):
    user_id =  features['id'].iloc[i]
    end_date = features['event_datetime'].iloc[i]
    first_date = end_date - T

    interval_features = features[(features['id'] == user_id) &
                                 (first_date <= features['event_datetime']) &
                                 (features['event_datetime'] < end_date)]

    sum_events = interval_features.sum(axis=0)
    sum_events.drop('_target', axis=0, inplace=True)
    sum_events.index = sum_events.index + '_'
    features.iloc[i].append(sum_events)
    return features.iloc[i].append(sum_events)

def add_last(features, T):   
    _cols = cols.append(events + '_')
    n_features = pd.DataFrame(columns = _cols, index = [])

    for i in features.index:
        new_row = sum_features_for(features, i, T)
        n_features.loc[i] = new_row

    return n_features
#%%     
###############################################################################
T = dt.timedelta(hours=24)
DELAY = 1 # Ts
INTEGRAL = 7
PLATFORM = 'ios'
INFILE = "data/events_%s.csv" % (PLATFORM)
OUTFILE = "data/%s_T=%sh_d=%dT_int=%s.csv" % (PLATFORM, get_hours(T), DELAY, INTEGRAL)
SOURCE_TARGET = 'process_orderSendSuccess_event'
TARGET = '_target'

print("extractions ", OUTFILE)

# input data
data = pd.read_csv(INFILE)
data = prepare_data(data)

# for output data
cols = pd.Series(['event_datetime', 'id', TARGET])
events = pd.Series(data['event_name'].unique())
cols = cols.append(events)
features = pd.DataFrame(columns = cols, index = [])

# cycle over date-time intervals
first_date = round_datetime(data['event_datetime'].iloc[0], T)
end_date = round_datetime(data['event_datetime'].iloc[-1], T) - T*DELAY
date_list = [first_date + x*T for x in range(0, (end_date-first_date+T)//T)]
#%% 
for interval_starts in date_list:
    cur_features = features_on_interval(data, interval_starts, T)
    next_features = features_on_interval(data, interval_starts + DELAY*T, T)
    delayd_features = fill_target(cur_features, next_features)
    features = features.append(delayd_features, ignore_index=True)

# clean
features.fillna(0, inplace=True)
 
if(INTEGRAL>0):
    features = add_last(features, dt.timedelta(days=INTEGRAL))
#%%
# out
features.rename(columns={SOURCE_TARGET:'_buy', SOURCE_TARGET+'_':'_buy_'}, inplace=True)
features = features.set_index(['event_datetime','id'])
features = features.reindex_axis(sorted(features.columns), axis=1)
features.to_csv(OUTFILE, sep=',', header=True) 