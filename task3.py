import pandas as pd
import pymongo
import mysql.connector as conn
from sqlalchemy import engine
import json

# 1) Read this dataset in pandas , mysql and mongodb
fit_df = pd.read_csv(r"C:\Users\user\Downloads\FitBit (3)-20220718T133838Z-001\FitBit (3)\FitBit\FitBit data.csv")
# print(fit_df)
fit_df.to_json("fitbit.json",orient='records')
# client = pymongo.MongoClient("mongodb+srv://UMESH:UMESH@cluster0.vpbql9v.mongodb.net/?retryWrites=true&w=majority")
# database = client["pandas_task"]
# collection = database["fitbit"]
# with open("fitbit.json") as file:
#     fitbit_json = json.load(file)
# collection.insert_many(fitbit_json)

# 2) while creting a table in mysql dont use manual approach to create it ,always use a automation to create a table in mysql
#hint - use csvkit library to automate this task and to load a data in bulk in you mysql

mysql_settings = {"host":"localhost","user":"root","passwd":"root","allow_local_infile":True}
mysql = conn.Connect(**mysql_settings)
cursor = mysql.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS pandas_task')
mysql_engin = engine.create_engine("mysql+pymysql://root:root@localhost/pandas_task")
# fit_df.to_sql('fitbit',con=mysql_engin)

# 3. convert all the dates available in dataset to timestamp format in pandas and in sql you to convert it in date format
fit_df['ActivityDate']= pd.to_datetime(fit_df['ActivityDate'])
# droping and creating again fitbit again
cursor.execute('USE pandas_task')
# cursor.execute('DROP TABLE IF EXISTS fitbit')
# fit_df.to_sql('fitbit',con=mysql_engin,index=False)
# columns_to_timestamp = ['VeryActiveMinutes','FairlyActiveMinutes','LightlyActiveMinutes','SedentaryMinutes']
# for column in columns_to_timestamp:
#     query = f"update fitbit set {column} = timestamp(ActivityDate,convert(CONCAT(cast(floor({column}/60)AS CHAR),':',cast({column}% 60 AS CHAR),':0'),time))"
#     cursor.execute(query)
#     query2 = f"alter table fitbit modify {column} TIMESTAMP"
#     cursor.execute(query2)
# mysql.commit()
# cursor.execute('DESC fitbit')
# print(cursor.fetchall())

# Converting minutes into time delta to perform operations on them
# fit_df['VeryActiveMinutes'] = pd.to_timedelta(fit_df['VeryActiveMinutes'],unit='m')
# fit_df['FairlyActiveMinutes'] = pd.to_timedelta(fit_df['FairlyActiveMinutes'],unit='m')
# fit_df['LightlyActiveMinutes'] = pd.to_timedelta(fit_df['LightlyActiveMinutes'],unit='m')
# print(fit_df.dtypes)

# 4. Find out in this data that how many unique id's we have
# print(len(fit_df['Id'].unique()))
# uni_id = "select distinct(Id) from pandas_task.fitbit"
# cursor.execute(uni_id)
# print(len(cursor.fetchall()))

#5 . which id is one of the active id that you have in whole dataset
#active person is considered as highest TotalSteps who is regular in activities
# print(fit_df.columns)

# print(fit_df.groupby('Id')['TotalSteps'].max().sort_values(ascending=False))
#
act_id_sql="select id from \
            (select id,sum(TotalSteps) tot_sum , sum(case when TotalSteps = 0 then 1 else 0 end) zero_step,rank() over(order by sum(TotalSteps) desc) rnk \
                from pandas_task.FitBit group by 1 having zero_step = 0 \
            ) a where rnk = 1"
cursor.execute(act_id_sql)
print("5.Most active id")
print(cursor.fetchall()[0][0])

# 6. how many of them have not logged there activity find out in terms of number of ids.
# person who use to covert the distance but they donot log it

# print(len(fit_df['Id'].unique()) - len(fit_df[fit_df['LoggedActivitiesDistance']!=0]['Id'].drop_duplicates()))

# No_Log_hr = "select count(id) from (select id, sum(LoggedActivitiesDistance) log_tot from pandas_task.FitBit group by 1 \
#                 having log_tot = 0.000000000000000) a "
#
# cursor.execute(No_Log_hr)
# print("6.how many of them have not logged there activity. Find out in terms of number of ids")
# print(cursor.fetchall()[0][0])



# 7. Find out who is the laziest person id that we have in dataset

# print(fit_df.groupby('Id')['Calories'].mean().sort_values().head(1))

# lazy_id = "select id from \
#                 (select id, count(distinct ActivityDate) cnt, rank() over(order by count(distinct ActivityDate)  asc) rnk \
#                 from pandas_task.FitBit group by 1 order by cnt asc) a \
#             where rnk = 1"
# cursor.execute(lazy_id)
# print("7 . Find out who is the laziest person id that we have in dataset")
# for i in cursor.fetchall():
#     print(i[0],sep=' ', end=' ')

# 8. Explore over an internet that how much calories burn is required for a healthy person and find out how many healthy person we have in our dataset
# Reference from https://www.goodto.com/wellbeing/diets-exercise/what-is-calorie-how-many-lose-weigt-425557
# The average person burns around 1800 calories a day doing absolutely nothing.
# print(fit_df[fit_df['Calories'] >= 1800]["Id"].drop_duplicates())
# print(len(fit_df[fit_df['Calories'] >= 1800]["Id"].drop_duplicates()))

#Considering calories to be burnt is 2200

# cal_health=("select count(Id) from pandas_task.fitbit where Calories > 2200")
# cursor.execute(cal_health)
# print(cursor.fetchall()[0][0])

# 9. how many person are not a regular person with respect to activity try to find out those

# print(fit_df.groupby('Id').describe()['Calories']['std'].sort_values(ascending=False).head(1))

# nt_regular = ("select count(distinct Id) from pandas_task.fitbit where TotalSteps = 0")
# cursor.execute(nt_regular)
# print(cursor.fetchall()[0][0])


# 10. who is the thired most active person in this dataset find out those in pandas and in sql both .

# print(fit_df.groupby('Id')['Calories'].max().sort_values(ascending=True).tail(3).head(1))

#active person is considered as highest TotalSteps with regular activities
# actv_id = "select id from \
#             (select id,sum(TotalSteps) tot_sum , sum(case when TotalSteps = 0 then 1 else 0 end) zero_step,rank() over(order by sum(TotalSteps) desc) rnk \
#                 from pandas_task.fitbit group by 1 having zero_step = 0 \
#             ) a where rnk = 3"
# cursor.execute(actv_id)
# print("10.third most active person in this dataset")
# print( cursor.fetchall()[0][0])

# 11. who is the 5th most laziest person avilable in dataset find it out

# print(fit_df.groupby('Id')['Calories'].mean().sort_values().head(5).tail(1))
#Lazy person is considered as Lowest TotalSteps with irregular activities
# laz_id = "select id from \
#             (select id,sum(TotalSteps) tot_sum , sum(case when TotalSteps = 0 then 1 else 0 end) zero_step,rank() over(order by sum(TotalSteps) asc) rnk \
#                 from pandas_task.fitbit group by 1 having zero_step <> 0 \
#             ) a where rnk = 5"
# cursor.execute(laz_id)
# print("11.Fifth most laziest person in this dataset")
# print(cursor.fetchall()[0][0])


# 12. what is a total acumulative calories burn for a person find out

# print(fit_df.groupby('Id')['Calories'].sum())

# cal_tot = "select id, sum(Calories) from pandas_task.fitbit group by 1"
# cursor.execute(cal_tot)
# print("12 . what is a total accumulative calories burn for a person find out")
# # print(cursor.fetchall())
# for i,j in cursor.fetchall():
#     print("Id - {} and Sum of Calories {}".format(i,j))
