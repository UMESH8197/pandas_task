import mysql.connector as conn
import pandas as pd
import pymongo
import json

client = pymongo.MongoClient("mongodb+srv://UMESH:UMESH@cluster0.vpbql9v.mongodb.net/?retryWrites=true&w=majority")
mongo_db=client.test

mysql_db = conn.connect(host = "localhost", user="root", passwd="root", allow_local_infile=True)
cursor = mysql_db.cursor(buffered=True)

#----------(1)Table creation---------------
cursor.execute("create table if not exists ineuron.Attribute1(Dress_ID int(10), Style varchar(50), Price varchar(10), Rating decimal(2,1), Size varchar(5), Season varchar(10), NeckLine varchar(20), SleeveLength varchar(20),waiseline varchar(20),	Material varchar(20),FabricType	varchar(20), Decoration	varchar(20), PatternType varchar(20),Recommendation int(1))")
cursor.execute("create table if not exists ineuron.dress_sales1(Dress_ID int(10), Sale_Day_1 int(10), Sale_Day_2 int(10), Sale_Day_3 int(10), Sale_Day_4 int(10),Sale_Day_5 int(10),Sale_Day_6 int(10),Sale_Day_7 int(10), Sale_Day_8 int(10),Sale_Day_9 int(10),Sale_Day_10 int(10),Sale_Day_11 int(10),Sale_Day_12 int(10), Sale_Day_13 int(10),Sale_Day_14 int(10),Sale_Day_15 int(10),Sale_Day_16 int(10),Sale_Day_17 int(10),Sale_Day_18 int(10), Sale_Day_19 int(10),Sale_Day_20 int(10),Sale_Day_21 int(10),Sale_Day_22 int(10),Sale_Day_23 int(10))")


ds="Load data local infile 'C:/Users/user/dress_sale.csv' into table ineuron.dress_sales1\
               fields terminated by ',' \
               Lines terminated by '\n' \
               ignore 1 lines\
               (Dress_ID, Sale_Day_1, Sale_Day_2,	Sale_Day_3,	Sale_Day_4, Sale_Day_5, Sale_Day_6,	Sale_Day_7, Sale_Day_8, Sale_Day_9, \
                Sale_Day_10, Sale_Day_11, \
                Sale_Day_12, Sale_Day_13, Sale_Day_14, Sale_Day_15, Sale_Day_16, Sale_Day_17, Sale_Day_18, Sale_Day_19,\
                Sale_Day_20, Sale_Day_21, Sale_Day_22, Sale_Day_23)"
cursor.execute(ds)
mysql_db.commit()

cursor.execute('select * from ineuron.dress_sales1')
df_dress_sale = pd.DataFrame(data=cursor.fetchall())
print(df_dress_sale)


#
# sql_stmt = 'SET GLOBAL local_infile = TRUE'
# cursor.execute(sql_stmt)

attr="Load data local infile 'C:/Users/user/attribute_dataset.csv' into table ineuron.Attribute_dataset\
               fields terminated by ',' \
               Lines terminated by '\n' \
               ignore 1 lines\
               (Dress_ID, Style, Price,	Rating,	Size, Season, NeckLine,	SleeveLength, waiseline, Material, FabricType, Decoration, \
                Pattern_Type, Recommendation)"

cursor.execute(attr)
mysql_db.commit()

cursor.execute('select * from ineuron.attribute1')
read_tb = pd.DataFrame(data=cursor.fetchall())
# print(read_tb)
attri_json = read_tb.to_json('attre.json')
# print(attri_json)

with open('attre.json','r') as file:
    file_data = json.load(file)

database = client['json_db']
collection = database['json_tb']
collection.insert_one(file_data)

cursor.execute("select a.*,b.* from ineuron.attribute1 a left join ineuron.dress_sales1 b on a.dress_id = b.dress_id")
# print(cursor.fetchall())

cursor.execute("select distinct(dress_id) from ineuron.attribute1")
# print(cursor.fetchall())
# print("\nUnique dress ids are :")
# for i in cursor.fetchall():
#     print(i[0], sep=',', end=',')

cursor.execute("select count(distinct dress_id) from ineuron.attribute1 where  Recommendation = 0")
# print(cursor.fetchall())
# print("\nTotal dresses with recommendation = 0 -->",cursor.fetchall()[0][0])


cursor.execute("select dress_id, sum(Sale_Day_1) + sum(Sale_Day_2) + sum(Sale_Day_3) + sum(Sale_Day_4) + sum(Sale_Day_5) +sum(Sale_Day_6) + sum(Sale_Day_7) + sum( Sale_Day_8) + sum(Sale_Day_9) + \
                sum(Sale_Day_10) + sum(Sale_Day_11) + \
                sum(Sale_Day_12) + sum(Sale_Day_13) + sum(Sale_Day_14) + sum(Sale_Day_15) + sum(Sale_Day_16) + sum(Sale_Day_17) + sum(Sale_Day_18) + sum(Sale_Day_19) +\
                sum(Sale_Day_20) + sum(Sale_Day_21) + sum(Sale_Day_22) + sum(Sale_Day_23) from ineuron.dress_sales1 group by dress_id")
# print(cursor.fetchall())
# for i, j in cursor.fetchall():
# print("Dress id {} with sale {}".format(i, j))

cursor.execute("select dress_id from  \
                        (select dress_id, rank() over(order by Tot_Sale desc) rnk from (\
                            select dress_id , sum(Sale_Day_1)+sum(Sale_Day_2)+sum(Sale_Day_3)+sum(Sale_Day_4)+sum(Sale_Day_5)+sum(Sale_Day_6)+ \
                            sum(Sale_Day_7)+sum(Sale_Day_8)+sum(Sale_Day_9)+sum(Sale_Day_10)+sum(Sale_Day_11)+sum(Sale_Day_12)+sum(Sale_Day_13)+ \
                            sum(Sale_Day_14)+sum(Sale_Day_15)+sum(Sale_Day_16)+sum(Sale_Day_17)+sum(Sale_Day_18)+sum(Sale_Day_19)+sum(Sale_Day_20)+ \
                            sum(Sale_Day_21)+sum(Sale_Day_22)+sum(Sale_Day_23) Tot_Sale \
                            from ineuron.dress_sales1 group by 1 \
                            )a \
                        )b \
                        where rnk=3"

               )
# print("\nDress id with third highest sales is --", cursor.fetchall()[0][0])


