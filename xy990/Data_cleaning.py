
# coding: utf-8

# In[2]:

'''
The purpose of this file is data cleaning and output clean datasets as new csv files
'''
import pandas as pd
import numpy as np
import os

data = pd.read_csv("NYPD_Motor_Vehicle_Collisions.csv", header = 0)


# In[ ]:

#Collision data cleaning

data16 = data[data['DATE'].str.contains("2016")]
a = np.where(data16['DATE'].str.startswith('11'))[0]
data16 = data16.drop(data16.index[a])
data15 = data[data['DATE'].str.contains("2015")]
for i in range(1, 10):
    a = np.where(data15['DATE'].str.startswith('0' + str(i)))[0]
    data15 = data15.drop(data15.index[a])
b = np.where(data15['DATE'].str.startswith(str(10)))[0]
data15 = data15.drop(data15.index[b])
frames = [data16, data15]
data = pd.concat(frames)

drop_label = ['ZIP CODE','LATITUDE','LONGITUDE','LOCATION','NUMBER OF PERSONS INJURED','NUMBER OF PERSONS KILLED','CONTRIBUTING FACTOR VEHICLE 2',
              'CONTRIBUTING FACTOR VEHICLE 3','CONTRIBUTING FACTOR VEHICLE 4','CONTRIBUTING FACTOR VEHICLE 5','VEHICLE TYPE CODE 2','VEHICLE TYPE CODE 3','VEHICLE TYPE CODE 4',
              'VEHICLE TYPE CODE 5', 'BOROUGH','CONTRIBUTING FACTOR VEHICLE 1','VEHICLE TYPE CODE 1']
day_plot_data = data.drop(drop_label, 1)
day_plot_data.to_csv('dayplot.csv')
data = data.drop(['UNIQUE KEY', 'ON STREET NAME', 'OFF STREET NAME', 'CROSS STREET NAME', 'NUMBER OF PEDESTRIANS INJURED', 'NUMBER OF PEDESTRIANS KILLED',
       'NUMBER OF CYCLIST INJURED', 'NUMBER OF CYCLIST KILLED',
       'NUMBER OF MOTORIST INJURED', 'NUMBER OF MOTORIST KILLED','TIME','ZIP CODE', 'LATITUDE', 'LONGITUDE'], axis = 1)

#drop rows with no location info
geomap = data[data['LOCATION'].notnull()]
geomap.to_csv("geomap.csv", sep = ',', index = False)


# In[ ]:

#For faster performance, create a new dataframe for storing immediate results
data = data.drop(['LOCATION'], axis = 1)
counts = data.groupby(['DATE']).size().to_frame()
counts.columns = ['TOTAL']
num_injured = data.groupby(by=['DATE'])['NUMBER OF PERSONS INJURED'].sum().to_frame()
num_killed = data.groupby(by=['DATE'])['NUMBER OF PERSONS KILLED'].sum().to_frame()

df = pd.concat([num_killed, num_injured, counts], axis = 1)
df = df.reset_index(level=['DATE'])

bor = data.groupby(['DATE','BOROUGH']).size().to_frame()
bor.columns = ['Counts']
bor = bor.reset_index(level=['BOROUGH'])
bor = bor.pivot(columns='BOROUGH', values='Counts') 
bor = bor.reset_index(level=['DATE'])
bor = bor.reset_index(drop=True)

df = pd.merge(df, bor)

factor = data.groupby(['DATE','CONTRIBUTING FACTOR VEHICLE 1']).size()
factor = factor.to_frame()
factor.columns=['factor']
factor = factor.reset_index(level=['CONTRIBUTING FACTOR VEHICLE 1','DATE'])
factor = factor[factor['CONTRIBUTING FACTOR VEHICLE 1'] != 'Unspecified']
maxfactor = factor.loc[factor.groupby(['DATE'])['factor'].idxmax()]['CONTRIBUTING FACTOR VEHICLE 1']
a = list(maxfactor)
b = ['Other'] * 53
df['MaxFactor'] = a + b

vehicle = data.groupby(['DATE','VEHICLE TYPE CODE 1']).size()
vehicle = vehicle.to_frame()
vehicle.columns=['car']
vehicle = vehicle.reset_index(level=['VEHICLE TYPE CODE 1','DATE'])
maxcar = vehicle.loc[vehicle.groupby(['DATE'])['car'].idxmax()]['VEHICLE TYPE CODE 1']
df['MaxVehicle'] = list(maxcar)

df.to_csv("clean_data.csv", sep=',', index = False)


# In[3]:

'''

WEATHER

'''

#Load and merge 12 weather datasets
merge = []
for filename in os.listdir('C:/Users/Aileen/Desktop/Weather/'):
    monthly = pd.read_table('C:/Users/Aileen/Desktop/Weather/' + filename, delim_whitespace = True)
    for i in range(0, len(monthly)):
        if np.isnan(monthly.iloc[i]['DR']):
            monthly.loc[i,'WX'] = 0
    monthly = monthly.drop(['MAX', 'MIN', 'DEP','HDD','CDD','12ZDPTH','AVGSPD','MXSPD','2MinDIR','MIN.1','PSBL','SPD','DR'], axis = 1)
    datelist = []
    for i in range(len(monthly)):
        datelist.append(str(filename)[0:2] + '/' + str('%02d' % int(monthly['DY'][i])) + '/' + str(filename)[3:7])
    monthly['DATE'] = datelist
    monthly = monthly.drop(['DY'], axis = 1)
    monthly = monthly[['DATE','AVG','WTR','SNW','S-S','WX']]
    merge.append(monthly)
weather = pd.concat(merge)

#Clean weather data
weather = weather.reset_index(drop = True)
weather = weather.replace(to_replace = 'T', value = '0.00')
weather = weather.replace(to_replace = 'M', value = '0')
weather.to_csv("weather.csv", sep=',', index = False)

