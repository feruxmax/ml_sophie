# -*- coding: utf-8 -*-
"""
Created on Sun Oct 23 03:10:39 2016

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
    for curr_id in myData_evidance['Identifier']:
        if curr_id == id_evidence['Identifier']:
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
            if (Data_evidance.ix[count,'Identifier'] == id_evidence['Identifier']) and (Data_evidance.ix[count, 'event_datetime'] == id_evidence['event_datetime']):
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
        new_line['Identifier'] = id_evidence['Identifier']
        new_line['event_datetime'] = first_day
        Data_evidance = Data_evidance.append(new_line, ignore_index=True)
        count_line_Data_evidance = count_line_Data_evidance + 1
        print("Good ", i)
        
#Отсортируем полученный фрейм по дате и по id
Data_evidance = Data_evidance.set_index(['event_datetime'])
print(Data_evidance)
#Выгрузим фрейм в файл
Data_evidance.to_csv("evidance_event2.csv", sep=',', header=True)        