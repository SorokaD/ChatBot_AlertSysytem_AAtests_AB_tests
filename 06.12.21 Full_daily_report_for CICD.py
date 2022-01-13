import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import io
from read_db.CH import Getch
import datetime
import pandahouse
import os

# задаем параметры бота
bot=telegram.Bot(token='5040852626:AAGIuHp8MMKegdiorCUzSzABVHNYvrdNGNQ')
chat_id=-708816698

# Запорс на количество лайков пользователей за 8 последних дней  -сегодня
data_views=Getch("select \
toStartOfInterval(time, INTERVAL 1 day) as t, \
count(user_id) as metric \
from simulator.feed_actions \
where toDate(time)>today()-8 and action='view' \
group by t \
order by t").df
data_views.drop(labels=[7], axis=0, inplace=True)

# Запорс на количество просмотров пользователей за 8 последних дней  -сегодня
data_likes=Getch("select \
toStartOfInterval(time, INTERVAL 1 day) as t, \
count(user_id) as metric \
from simulator.feed_actions \
where toDate(time)>today()-8 and action='like' \
group by t \
order by t").df
data_likes.drop(labels=[7], axis=0, inplace=True)

# Запорс на количество сообщений пользователей за 8 последних дней  -сегодня
data_mess=Getch("select \
toStartOfInterval(time, INTERVAL 1 day) as t, \
count(user_id) as metric \
from simulator.message_actions \
where toDate(time)>today()-8 \
group by t \
order by t").df
data_mess.drop(labels=[7], axis=0, inplace=True)

# запрос на ПАССИВНЫХ(только просмотры) пользователей за 8 последних дней  -сегодня
data_pass=Getch("select \
toStartOfInterval(time, INTERVAL 1 day) as t, \
uniq(user_id) as metric, \
median(age) as age_median \
from simulator.feed_actions \
where toDate(time)>today()-8 and action='view' \
group by t \
order by t").df
data_pass.drop(labels=[7], axis=0, inplace=True)

# запрос на АКТИВНЫХ(лайки и переписка) пользователей за 8 последних дней  -сегодня
data_act=Getch("select \
toStartOfInterval(time, INTERVAL 1 day) as t, \
uniq(user_id) as metric, \
median(age) as age_median \
from simulator.feed_actions \
right outer join simulator.message_actions \
on simulator.feed_actions.user_id=simulator.message_actions.user_id \
where toDate(time)>today()-8 and action='like' \
group by t \
order by t").df
data_act.drop(labels=[7], axis=0, inplace=True)

# вычисляем коэфициент активности
koef_act=pd.DataFrame()
koef_act['t']=data_pass['t']
koef_act['metric']=round(data_act['metric']/data_pass['metric'],4)

# запрос на количество активнойсей по городам views&likes за весь период
city_activ_feed=Getch("select \
city, \
count(city) as activity \
from simulator.feed_actions \
group by city \
order by activity desc \
limit 100").df

# запрос на количество активностей по городам messages за весь период
city_activ_mess=Getch("select \
city, \
count(city) as activity \
from simulator.message_actions \
group by city \
order by activity desc \
limit 100").df

# запрос на уникальных пользователей по городам за весь период
uniq_users_activ=Getch("select \
city, \
uniq(user_id) as uniq_users \
from simulator.feed_actions \
inner join simulator.message_actions \
on simulator.feed_actions.user_id=simulator.message_actions.user_id \
where action='like' \
group by city \
order by uniq_users desc \
limit 100").df

# формируем таюлицу с ТОР-10 самых активных городов
top_city_activ=city_activ_mess.append(uniq_users_activ.append(city_activ_feed)).groupby('city').aggregate('sum').sort_values('activity', ascending=False)
top_city_10=round(top_city_activ.iloc[0:9,:]/1000,2).astype(str)+'k'

# описываем функцию отправки графиков 
def send_graph(graph_title, line_title, data):
    plt.figure(figsize=(8,5))
    sns.lineplot(data=data, x='t', y='metric', label=line_title)
    plt.title(graph_title)
    plot_obj=io.BytesIO()
    plt.savefig(plot_obj)
    plot_obj.name='temp_plot.png'
    # перенесем курсор в началло файла что бы весь его увидеть
    plot_obj.seek(0)
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_obj)
    
# выводим блок "активности юзеров"
plt.figure(figsize=(8,5))
sns.lineplot(x='t', y='metric', data=data_act, label='Active uniq users')
sns.lineplot(x='t', y='metric', data=data_pass, label='Passive uniq users')
plt.title('Active & passive users')
plot_obj=io.BytesIO()
plt.savefig(plot_obj)
plot_obj.name='temp_plot.png'

# перенесем курсор в началло файла что бы весь его увидеть
plot_obj.seek(0)
plt.close()
bot.sendPhoto(chat_id=chat_id, photo=plot_obj)
send_graph('Line of coef. activity','koef_activity',koef_act)

# Выводим блок лайки просмотры сообщения
plt.figure(figsize=(8,5))
sns.lineplot(x='t', y='metric', data=data_views, label='views')
sns.lineplot(x='t', y='metric', data=data_likes, label='likes')
plt.title('Like & views')
plot_obj=io.BytesIO()
plt.savefig(plot_obj)
plot_obj.name='temp_plot.png'

# перенесем курсор в началло файла что бы весь его увидеть
plot_obj.seek(0)
plt.close()
bot.sendPhoto(chat_id=chat_id, photo=plot_obj)
send_graph('Messages','messages',data_mess)

# Выводим таблицу ТОР10 активных городов 
file_obj=io.StringIO()
top_city_10.to_csv(file_obj)
file_obj.seek(0)
file_obj.name='ТОР10 active city.csv'
bot.sendDocument(chat_id=chat_id, document=file_obj)
