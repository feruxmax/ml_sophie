# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 23:06:30 2016

@author: Лихотин
"""

import pandas as panda
from datetime import datetime

data = panda.read_csv('merge.csv')
date = data.event_datetime

temp = date[0].split(' ')
first_day = datetime.strptime(temp[0],'%Y-%m-%d')

Sample = data['event_name'].drop_duplicates()

Data_evidance = panda.DataFrame(columns = [d for d in Sample], index = [])
Data_evidance['event_datetime'] = 0
Data_evidance['Identifier'] = 0

id_evidence = {}
id_evidence = data.ix[0]

new_line = {}
for curr_Data_evidance in Data_evidance.columns:
    new_line[curr_Data_evidance] = 0

new_line['Identifier'] = id_evidence['Identifier']
new_line['event_datetime'] = id_evidence['event_datetime']
Data_evidance = Data_evidance.append(new_line, ignore_index=True)

for i,curr_day in enumerate(date):
    
    id_evidence = {}
    id_evidence = data.ix[i]
    
    id_evidence['event_datetime'] = first_day
    
    #Счётчики 
    count = 0
    count_line_Data_evidance = 0
    
    
    temp = date[i].split(' ')
    days = datetime.strptime(temp[0],'%Y-%m-%d')
    
    if first_day != days:        
        id_evidence['event_datetime'] = days
        
    myData_evidance = Data_evidance[Data_evidance['event_datetime'] == first_day]
    flag1 = False       #ios
    flag2 = False       #android
    for curr_id in myData_evidance['android_id']:
        if (id_evidence['android_id'] == 'NaN') and (id_evidence['ios_ifv'] != 'NaN'):
            if curr_id == id_evidence['Identifier']:
                flag1 = True
                break
        elif (id_evidence['android_id'] != 'NaN') and (id_evidence['ios_ifv'] == 'NaN'):    
            if curr_id == id_evidence['Identifier']:
                flag2 = True
                break
        else:
            print("ERROR_1")
    
    
    if (flag1 or flag2) and (first_day == days) :
        #К уже найденному id делаем суммирование всех его признаков
        #В нашем Фрейме нужно найти строку с необходимом id и соответствующей датой
        siz = Data_evidance.index.size #Подсчитвываем количество признаков во фрейме для цикла
        while siz != count:
            #Находим ту строчку где записан наш id и соответствующая к нему дата
            #и в соответствующее поле фрейма прибавляем 1
            if flag2:
                if (Data_evidance.ix[count,'Identifier'] == id_evidence['android_id']) and (Data_evidance.ix[count, 'event_datetime'] == id_evidence['event_datetime']):
                        for curr_Data_evidance in Data_evidance.columns:
                            if id_evidence['event_name'] == curr_Data_evidance:
                                summ = Data_evidance.ix[count, id_evidence['event_name']]
                                Data_evidance.ix[count, id_evidence['event_name']] = summ + 1  
                                break;
                count = count + 1;
            elif flag1:
                if (Data_evidance.ix[count,'Identifier'] == id_evidence['ios_ifv']) and (Data_evidance.ix[count, 'event_datetime'] == id_evidence['event_datetime']):
                        for curr_Data_evidance in Data_evidance.columns:
                            if id_evidence['event_name'] == curr_Data_evidance:
                                summ = Data_evidance.ix[count, id_evidence['event_name']]
                                Data_evidance.ix[count, id_evidence['event_name']] = summ + 1  
                                break;
                count = count + 1;
            else:
                print("ERROR_2")
    else:
        #Заготавливаем новый id с его признаками или новый день
        #Соответственно новую строку добавляем в конец Фрейма
        first_day = days
        new_line = {}
        for curr_Data_evidance in Data_evidance.columns:
            if id_evidence['event_name'] == curr_Data_evidance:
                new_line[curr_Data_evidance] = 1
            else:
                new_line[curr_Data_evidance] = 0
        if flag1:
            new_line['Identifier'] = id_evidence['ios_ifv']
        elif flag2:
            new_line['Identifier'] = id_evidence['android_id']
        else:
            print("ERROR_3")
        new_line['event_datetime'] = first_day
        Data_evidance = Data_evidance.append(new_line, ignore_index=True)
        count_line_Data_evidance = count_line_Data_evidance + 1
        print("Good ", i)
        
#Отсортируем полученный фрейм по дате и по id
Data_evidance = Data_evidance.set_index(['event_datetime','android_id'])
print(Data_evidance)
#Выгрузим фрейм в файл
Data_evidance.to_csv("evidance_event_v2.csv", sep=',', header=True)        