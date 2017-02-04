'''
Created on Dec 15, 2016

@author: twff
'''
import pandas as pd
import numpy as np
from itertools import groupby
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA
import matplotlib.pyplot as plt
import collections as col
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter

#data clean_data source
def plot_month_total_line(df):
    g = df['TOTAL'].groupby(df['date']).sum()
    g = pd.DataFrame(g)
    g.plot()
    plt.title("Year Collisions by Month")
    plt.xlabel("Month")
    plt.ylabel("Number of Collisions")
    plt.grid(True)
    plt.show()
    
def turn_date_to_int(x):
    '''
    Turn the string type date to int
    --------------------------------
    return int x
    '''
    if type(x)==str:
        x = x[-4:-1]+x[-1]+x[0:2]
    return x

def turn_day_to_int(x):
    if type(x)==str:
        x = x[-4:-1]+x[-1]+x[0:2]+x[3:5]
    return x

def plot_whole_year(df):
    '''
    Plot the whole year collision lineplot
    --------------------------------------
    show graph
    '''
    days = pd.DataFrame(df['DATE'])
    days['TOTAL'] = df['TOTAL']
    days.plot()
    plt.title("Year Collisions by Day")
    plt.xlabel('Days')
    plt.ylabel("Number of Collisions")
    plt.grid(True)
    plt.show()

def pie_borough(pie):
    '''
    Plot the pie chart of the number of collision in five boroughs in NYC
    ---------------------------------------------------------
    input Dataframe
    return plot
    '''
    borough = pie[["BRONX", 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']]
    sum_by_borough = pd.DataFrame.sum(borough, axis =0)
    sum_by_borough['BRONX']

    labels = 'BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND'
    sizes = pd.Series.tolist(sum_by_borough)
    colors = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue','red']
    explode = (0, 0.1, 0, 0, 0)  # explode second slice

    # Plot
    plt.pie(sizes, explode=explode, labels=labels, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')
    plt.show()

def contributing_fator_bar(bar_data, bar_width = 0.35, n_groups = 47):
    '''
    Plot the bar chart of the contributing factors for car collision
    ------------------------------
    input: bar_data dataframe
           bar_width float
           n_groups int
           
    return graph 
    '''
    counts = bar_data.groupby(['CONTRIBUTING FACTOR VEHICLE 1']).agg(len)
    counts = counts.drop(['BOROUGH','VEHICLE TYPE CODE 1'],1)
    
    fig, ax = plt.subplots(1)
    index = np.arange(n_groups)
    plt.rcParams['figure.figsize'] = 20, 12

    plt.bar(range(47), counts['DATE'].drop('Unspecified',0))
    plt.title('Contributing Factor', fontsize = 30)
    plt.xlabel('Factors', fontsize = 20)
    plt.ylabel('Number of Collision', fontsize = 20)
    plt.xticks(index + bar_width, counts.index,rotation='vertical',fontsize=10)
    plt.show()
    
    
def vehicle_type_bar(bar_data, bar_width = 0.35, n_groups = 16):
    '''
    plot the bar chart of the vehicle types for car collision in NYC
    _______________________________________
    input: bar_data dataframe
           bar_width float
           n_groups int
           
    return graph 
        
    '''
    plt.rcParams['figure.figsize'] = 12, 10
    fig, ax = plt.subplots(1)
    index = np.arange(n_groups)
    
    vcounts = bar_data.groupby(['VEHICLE TYPE CODE 1']).agg(len)
    vcounts = vcounts.drop(['BOROUGH','CONTRIBUTING FACTOR VEHICLE 1'],1)

    plt.bar(range(16), vcounts['DATE'].drop('UNKNOWN',0))
    plt.title('Vehicle Type', fontsize = 30)
    plt.ylabel('Number of Collision', fontsize = 20)
    plt.xlabel('Vehicle Type',fontsize = 20)
    plt.xticks(index + bar_width, vcounts.index, rotation= 'vertical')
    plt.show()
