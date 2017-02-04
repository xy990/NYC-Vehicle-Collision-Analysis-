'''
Created on Dec 15, 2016

@author: twff
'''
import pandas as pd
import numpy as np
import collections as col
from collections import Counter
from datetime import datetime
import matplotlib.pyplot as plt

def check(inputs):
    """Check whether inputs are valid, and create the class"""
    #initial checks
    inputs = inputs.strip()
    inputs = inputs.replace(" ", "")
    sep = inputs.split("/")
    if len(sep) != 3 and len(sep) != 2:
        print("This is an invalid input.")
        raise ValueError
    if sep[-1] != '2015' and sep[-1] != '2016':
            print("This is an invalid input. Please input year 2015 or 2016")
            raise ValueError
    if len(sep) == 3:
        if len(sep[0]) != 2 or len(sep[1]) != 2 or len(sep[2]) != 4:
            print("This is an invalid input. The format should be: mm/dd/yyyy")
            raise ValueError
        try:
            int(sep[0]) + int(sep[1]) + int(sep[2])
        except ValueError:
            print("Please enter a valid date.")
            raise ValueError
        if sep[2] == '2015':
            if sep[0] != '11' and sep[0] != '12':
                print("Please input a valid date range (11.2015 till 10.2016).")
                raise ValueError
            if sep[0] == '11':
                if int(sep[1]) > 30 or int(sep[1]) < 1:
                    print("Please input date accordingly.")
                    raise ValueError
            if sep[0] == '12':
                if int(sep[1]) > 31 or int(sep[1]) < 1:
                    print("Please input date accordingly.")
                    raise ValueError
        if sep[2] == '2016':
            odd = [1, 3, 5, 7, 8, 10, 12]
            even = [4, 6, 9, 11]
            if int(sep[0]) >= 11:
                print("Please input a valid date range (11.2015 till 10.2016).")
                raise ValueError
            if int(sep[0]) in odd:
                if int(sep[1]) > 31 or int(sep[1]) < 1:
                    print("Please input date accordingly.")
                    raise ValueError
            if int(sep[0]) in even:
                if int(sep[1]) > 30 or int(sep[1]) < 1:
                    print("Please input date accordingly.")
                    raise ValueError
            if int(sep[0]) == 2:
                if int(sep[1]) > 29 or int(sep[1]) < 1:
                    print("Please input date accordingly.")
                    raise ValueError
                
    if len(sep) == 2:
        if len(sep[0]) != 2 or len(sep[1]) != 4:
            raise ValueError("This is an invalid input. The format should be: mm/yyyy")
        try:
            int(sep[0]) + int(sep[1])
        except ValueError:
            print("Please enter a valid date.")
            raise ValueError
        if sep[1] == '2015':
            if sep[0] != '11' and sep[0] != '12':
                print("Please input a valid date range (11.2015 till 10.2016).")
                raise ValueError
        if sep[1] == '2016':
            if int(sep[0]) >= 11:
                print("Please input a valid date range (11.2015 till 10.2016).")
                raise ValueError
                



def plot_month_plot(picked_month, data):
    '''
    Plot collisions in a month
    '''
    index = []
    for i in range(0, len(data)):
        if data.iloc[i]['DATE'].startswith(picked_month[0:2]):
            index.append(i)
    g = data.loc[index]['TOTAL']
    labels = [str(i) for i in range(1, len(index) + 1 , 1)]
    g.plot(linewidth = 2)
    plt.title("Collisions in the month " + str(picked_month), fontsize = 30)
    plt.xticks(index, labels)
    plt.xlabel("Days in Month", fontsize = 20)
    plt.ylabel("Number of Collisions", fontsize = 20)
    plt.rcParams['xtick.labelsize'] = 20
    plt.rcParams['ytick.labelsize'] = 20
    plt.grid(True)
    plt.show()

def plot_everyday_line(picked_date, df):
    '''
    Covert str to datetime type
    Plot everyday lineplot
    '''
    picked = df.loc[df['DATE'] == picked_date].copy() #select days
    picked['clock'] = picked['TIME'].map(lambda x: datetime.strptime(x, '%H:%M'))
    time = col.Counter(picked['clock'])
    time = pd.DataFrame.from_dict(time, orient='index')
    time = time.sort_index()
    plt.plot(time.index, time[0], linewidth = 2, color = "b")
    plt.title("Collisions Timeline at " + str(picked_date), fontsize = 30)
    plt.ylabel("Number of Collisions", fontsize = 20)
    plt.rcParams['xtick.labelsize'] = 20
    plt.rcParams['ytick.labelsize'] = 20
    plt.grid(True)
    plt.show()


# In[5]:

def weatheroutput(weather, date):
    
    '''
    Function when input a date in format "mm/dd/yyyy", should output the weather details of that day
    '''
    date = str(date)
    wxlist = ['fog','fog reducing visibility','thunder','ice pellets','hail','freezing rain or drizzle','duststorm, which make visibility less than 0.5 mile',
              'smoke or haze','blowing snow', 'tornado']
    if len(date) == 10:
        rainfall = 'no'
        snowfall = 'no'
        sky = 'clear'
        wx = []
        for i in range(0, len(weather)):
            if weather.iloc[i]['DATE'] == date:
                if float(weather.iloc[i]['WTR']) > 0:
                    rainfall = float(weather.iloc[i]['WTR'])
                    if rainfall <= 0.098:
                        rainfall = 'light'
                    elif rainfall <= 0.39:
                        rainfall = 'moderate'
                    else:
                        rainfall = 'heavy'
                if float(weather.iloc[i]['SNW']) > 0:
                    snowfall = 'some'
                if int(weather.iloc[i]['S-S']) <= 3:
                    sky = 'clear'
                elif int(weather.iloc[i]['S-S']) <= 7:
                    sky = 'partly cloudy'
                else:
                    sky = 'cloudy'
                if weather.iloc[i]['WX'] == 'X':
                    wx.append(wxlist[-1])
                else:
                    for index in range(1, 10):
                        if str(index) in str(weather.iloc[i]['WX']):
                            wx.append(wxlist[index - 1])

        print("\nOn " + date + ", the weather in New York is: ")
        print("  The temperature was " + str(weather.iloc[i]['AVG']) + ' degrees Fahrenheit.')
        print("  There was " + rainfall + " rainfall and " + snowfall + ' snowfall.')
        print("  The sky was " + sky + ".")
        if wx:
            print("  There was also " + ', and '.join(wx) + '.' + '\n')
                    
                
    if len(date) == 7:
        temp = []
        rain = 0
        snow = 0
        sky = []
        wx = []
        for i in range(0, len(weather)):
            if str(weather.iloc[i]['DATE'])[0:3] + str(weather.iloc[i]['DATE'])[-4:] == date:
                temp.append(weather.iloc[i]['AVG'])
                if float(weather.iloc[i]['WTR']) > 0:
                    rain = rain + 1
                if float(weather.iloc[i]['SNW']) > 0:
                    snow = snow + 1
                if int(weather.iloc[i]['S-S']) <= 3:
                    sky.append('clear')
                elif int(weather.iloc[i]['S-S']) <= 7:
                    sky.append('partly cloudy')
                else:
                    sky.append('cloudy')
                if weather.iloc[i]['WX'] == 'X':
                    wx.append(wxlist[-1])
                else:
                    for ind in range(1, 10):
                        if str(ind) in str(weather.iloc[i]['WX']):
                            wx.append(wxlist[ind - 1])

        sky = Counter(sky).most_common(1)[0][0] if Counter(sky) else None
        w = Counter(wx).most_common(1)[0][0] if Counter(wx) else None
        print("\nIn the month " + date + ", the overall weather in New York is: ")
        print("  The average daily temperature was " + str("%.1f" % np.mean(temp)) + ' degrees Fahrenheit.')
        print("  There were " + str(rain) + " rainy days and " + str(snow) + ' snowy days during the month.')
        print("  The sky was " + str(sky) + " most of the days.")
        if w:
            print("  During the month, there was also " + str(w) + ' for ' + str(Counter(wx).most_common(1)[0][1]) + ' days.' + '\n')


# In[6]:

def collisionoutput(date, data, dayplot): 
    
    '''Function when input a date in format "mm/dd/yyyy", should output the collisions details of that day'''
    date = str(date)
    if len(date) == 10:
        for i in range(0, len(data)):
            if data.iloc[i]['DATE'] == date:
                count = data.iloc[i]['TOTAL']
                manhat = data.iloc[i]['MANHATTAN']
                brook = data.iloc[i]['BROOKLYN']
                queen = data.iloc[i]['QUEENS']
                bronx = data.iloc[i]['BRONX']
                staten = data.iloc[i]['STATEN ISLAND']

                num_killed = int(data.iloc[i]['NUMBER OF PERSONS KILLED'])
                num_injured = int(data.iloc[i]['NUMBER OF PERSONS INJURED'])
                fact = data.iloc[i]['MaxFactor']
                vehicle = data.iloc[i]['MaxVehicle']
        
        print("\nOn " + date + ', the number of collisions in NYC is ' + str(count))
        print("  Number of collisions in Manhattan: " + str(manhat))
        print("  Number of collisions in Brooklyn: " + str(brook))
        print("  Number of collisions in Queens: " + str(queen))
        print("  Number of collisions in Bronx: " + str(bronx))
        print("  Number of collisions in Staten Island: " + str(staten))

        print("  There was " + str(num_injured) + " people injured, " + "and " + str(num_killed) + " people killed. ")
        print("  The factor that contributed most to these accidents was: " + str(fact))
        print("  The vehicle type that involved in most of the accidents was: " + str(vehicle).title())
        plot_everyday_line(date, dayplot)
            
    if len(date) == 7:
        count = manhat = brook = queen = bronx = staten = num_killed = num_injured = 0
        store = []
        for i in range(0, len(data)):
            if str(data.iloc[i]['DATE'])[0:3] + str(data.iloc[i]['DATE'])[-4:] == date:
                store.append(i)
                count = int(data.iloc[i]['TOTAL']) + count
                manhat = int(data.iloc[i]['MANHATTAN']) + manhat
                brook = int(data.iloc[i]['BROOKLYN']) + brook
                queen = int(data.iloc[i]['QUEENS']) + queen
                bronx = int(data.iloc[i]['BRONX']) + bronx
                staten = int(data.iloc[i]['STATEN ISLAND']) + staten

                num_killed = int(data.iloc[i]['NUMBER OF PERSONS KILLED']) + num_killed
                num_injured = int(data.iloc[i]['NUMBER OF PERSONS INJURED']) + num_injured
        fact = data.iloc[store]['MaxFactor'].value_counts().index[0]
        vehicle = data.iloc[store]['MaxVehicle'].value_counts().index[0]
        print("\nIn the month " + date + ', the number of collisions in NYC is ' + str(count))
        
        print("  Number of collisions in Manhattan: " + str(manhat))
        print("  Number of collisions in Brooklyn: " + str(brook))
        print("  Number of collisions in Queens: " + str(queen))
        print("  Number of collisions in Bronx: " + str(bronx))
        print("  Number of collisions in Staten Island: " + str(staten))

        print("  There was " + str(num_injured) + " people injured, " + "and " + str(num_killed) + " people killed. ")
        print("  The factor that contributed most to these accidents was: " + str(fact))
        print("  The vehicle type that involved in most of the accidents was: " + str(vehicle).title())

        plot_month_plot(date, data)

