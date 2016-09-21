# -*- coding: utf-8 -*-
"""
Created on Mon Sep  5 14:54:32 2016

@author: Лихотин
"""

import pandas as panda
from datetime import datetime

data = panda.read_csv('events_android.csv') 
date = data.event_datetime

#Инициализируем первую дату в нашем документе 
#Время не учитываем, только дата, соответственно далее работаем только по дням
temp = date[0].split(' ')
first_day = datetime.strptime(temp[0],'%Y-%m-%d')

#Осуществляем выборку, по действиям, которые совершали android_id(пользователи)
#и удаляем повторяющиеся действия, для того чтобы создать список с ключами событий
Sample = data['event_name'].drop_duplicates()
#Создаём DataFrame, являющийся результатом работы, в котором столбцы являются ключами событий из уже обработанной выборки
Data_evidance = panda.DataFrame(columns = [d for d in Sample], index = [])
#Дополняем наш фрейм допольнительными полями, в которых будет id андроида и днём совершения действия, инициализируя их нулями
Data_evidance['android_id'] = 0
Data_evidance['event_datetime'] = 0

#словарь в который будет записываться каждый раз новая строка из нашего файла для дальнейших проверок и работы
#инициализаруем его первой строкой
id_evidence = {}
id_evidence = data.ix[0]

#Здесь инициализируем нулями первую запись нашего фрейма
#Инициализирем словарь, который будет являться новой строкой во фрейме
new_line = {}
for curr_Data_evidance in Data_evidance.columns:
    new_line[curr_Data_evidance] = 0
#Также здесь записываем что это за id и в какой день была сделана операция, соответственно взятая тоже из первой записи в файле
new_line['android_id'] = id_evidence['android_id']
new_line['event_datetime'] = first_day
Data_evidance = Data_evidance.append(new_line, ignore_index=True)

#Работаем с записями в файле соответственно количество их равно количеством event_datetime записанную в date
for i,curr_day in enumerate(date):
    #здесь работаем с каждой записью в документе
    #дни необходимо соответственно обработать
    id_evidence = {}
    id_evidence = data.ix[i]
    #В документе очень много проеделанных операций за один день и разными пользователями
    #как изменяется день в файле только тогда и поменяется дата в first_day
    id_evidence['event_datetime'] = first_day
    
    #Счётчики 
    count = 0
    count_line_Data_evidance = 0
    
    #Обрабатываем время совершения операций. 
    #Переводим их только в дату совершения действия
    temp = date[i].split(' ')
    days = datetime.strptime(temp[0],'%Y-%m-%d')
    
    #Осуществляем проверку на то, изменился ли день или оставить старую дату
    #Это необходимо в дальнейшем для нашего фрейма, нужно ли созавать новую строку или обрабатываем уже существующую
    if first_day != days:        
        id_evidence['event_datetime'] = days
        
    #Осуществляем выборку в нашем фрейме по дням для проверки.
    #Её суть в том чтобы проверить совершал ли текущий(очередной из файла) id операцию или нет
    #В дальнейшем flag, отвечающий за эту операцию будет определять, нужно ли добавлять новую строку во фрейм
    #или обработать уже существующую
    myData_evidance = Data_evidance[Data_evidance['event_datetime'] == first_day]
    flag = False
    for curr_id in myData_evidance['android_id']:
        if curr_id == id_evidence['android_id']:
            flag = True
            break
    
    #Если пользователь не совершал каких-либо действий за этот день, то для него нужно сделать новую запись
    #или существующий ползователь совершил действие, но в другой день, то соответственно для него тоже
    if flag and (first_day == days) :
        #К уже найденному id делаем суммирование всех его признаков
        #В нашем Фрейме нужно найти строку с необходимом id и соответствующей датой
        siz = Data_evidance.index.size #Подсчитвываем количество признаков во фрейме для цикла
        while siz != count:
            #Находим ту строчку где записан наш id и соответствующая к нему дата
            #и в соответствующее поле фрейма прибавляем 1
            if (Data_evidance.ix[count,'android_id'] == id_evidence['android_id']) and (Data_evidance.ix[count, 'event_datetime'] == id_evidence['event_datetime']):
                    for curr_Data_evidance in Data_evidance.columns:
                        if id_evidence['event_name'] == curr_Data_evidance:
                            summ = Data_evidance.ix[count, id_evidence['event_name']]
                            Data_evidance.ix[count, id_evidence['event_name']] = summ + 1  
                            break;
            count = count + 1;
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
        new_line['android_id'] = id_evidence['android_id']
        new_line['event_datetime'] = first_day
        Data_evidance = Data_evidance.append(new_line, ignore_index=True)
        count_line_Data_evidance = count_line_Data_evidance + 1
        print("Good ", i)
        
#Отсортируем полученный фрейм по дате и по id
Data_evidance = Data_evidance.set_index(['event_datetime','android_id'])
print(Data_evidance)
#Выгрузим фрейм в файл
Data_evidance.to_csv("evidance_event_android.csv", sep=',', header=True)        

'''
for curr_day in date:
    temp = date[i].split(' ')
    #temp = temp[0].split('-') 
    #a = datetime.date(int(temp[0]),int(temp[1]),int(temp[2])
    days = datetime.strptime(temp[0],'%Y-%m-%d')    
    date_norm.append(days)
    i += 1
    
sample_days = date_norm[i-1] - date_norm[0]
j = 0
for day in range(sample_days.days):
    #С каждой итерацией меняется день
    diff_day = date_norm[j].day
    count = 0
    #Перебираем все записи на этот день
    while diff_day == date_norm[count+j].day:        
        count = count + 1    
    print(date_norm[count+j].day)
    j = count + j
'''