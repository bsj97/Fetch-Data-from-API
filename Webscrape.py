import requests
import json
import mysql.connector

'''
This API URL consists global and country wise covid data of the current date.
We will fetch and store it in 2 tables "GlobalData" & "CountryData" into mysql
'''  

URL = "https://api.covid19api.com/summary"
r = requests.get(URL)
  

# Fetching Global Data
global_data = r.json()['Global']
newconfirmed= global_data['NewConfirmed']
totalconfirmed= global_data['TotalConfirmed']
newdeaths= global_data['NewDeaths']
totaldeaths= global_data['TotalDeaths']
newrecovered= global_data['NewRecovered']
totalrecovered= global_data['TotalRecovered']
date= global_data['Date']


# Fetching Country Wise Data
countries= r.json()['Countries']

country_data=[]
for c in countries:
    id= c['ID']
    country= c['Country']
    countrycode= c['CountryCode']
    slug= c['Slug']
    newconfirmed= c['NewConfirmed']
    totalconfirmed= c['TotalConfirmed']
    newdeaths= c['NewDeaths']
    totaldeaths= c['TotalDeaths']
    newrecovered= c['NewRecovered']
    totalrecovered= c['TotalRecovered']
    date= c['Date']

    country_data.append((id, country, countrycode, slug, newconfirmed, totalconfirmed, newdeaths, totaldeaths, newrecovered, totalrecovered, date))


# Creating tables in mysql database
connection = mysql.connector.connect(host='localhost', database='Enter Your Database Name', user='root', password='Enter Your Passeord')
cursor = connection.cursor()

GlobalData_Create_Table = """CREATE TABLE GlobalData (
NewConfirmed int(20), 
TotalConfirmed int(20), 
NewDeaths int(20), 
TotalDeaths int(20), 
NewRecovered int(20), 
TotalRecovered int(20), 
Date varchar(250)) """

cursor.execute(GlobalData_Create_Table)

CountryData_Create_Table = """CREATE TABLE CountryData (
ID varchar(250),
Country varchar(250),
CountryCode varchar(250),
Slug varchar(250),
NewConfirmed int(20), 
TotalConfirmed int(20), 
NewDeaths int(20), 
TotalDeaths int(20), 
NewRecovered int(20), 
TotalRecovered int(20), 
Date varchar(250)) """

cursor.execute(CountryData_Create_Table)

print("Tables are created successfully ")

# Insert CountryData into mysql database
sql1 = "INSERT INTO CountryData (ID, Country, CountryCode, Slug, NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered, Date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

country_data_set = country_data

cursor.executemany(sql1, country_data_set)
connection.commit()
print(cursor.rowcount, "Record inserted successfully into CountryData table")


# Insert GlobalData into mysql database
sql2 = "INSERT INTO GlobalData (NewConfirmed, TotalConfirmed, NewDeaths, TotalDeaths, NewRecovered, TotalRecovered, Date) VALUES (%s, %s, %s, %s, %s, %s, %s)"

global_data_set = (newconfirmed, totalconfirmed, newdeaths, totaldeaths, newrecovered, totalrecovered, date)


cursor.execute(sql2, global_data_set)
connection.commit()
print(cursor.rowcount, "Record inserted successfully into GlobalData table")

# Close Connection
cursor.close()
connection.close()
print("Your connection is closed")

