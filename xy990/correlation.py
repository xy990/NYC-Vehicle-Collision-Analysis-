'''
Created on Dec 15, 2016

@author: twff

'''
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA

class correlation:
    def __init__(self, rangen=13, label = 'DATE'):
        self.rangen = rangen
        self.label = label
    
    def weather_corr(self, weather, data):
        total_rain = []
        for i in range(1, self.rangen):
            rain = 0
            a = np.where(weather['DATE'].str.startswith('%02d' % i))[0]
            for day in weather['WTR'][a]:
                rain = rain + float(day)
            total_rain.append(rain)
            
        #Total snow of each month
        total_snow = []
        for i in range(1, self.rangen):
            snow = 0
            a = np.where(weather['DATE'].str.startswith('%02d' % i))[0]
            for day in weather['SNW'][a]:
                snow = snow + float(day)
            total_snow.append(snow)
            
        collisions = []
        for i in range(1, self.rangen):
            num = sum(data['TOTAL'][data['DATE'].str.startswith('%02d' % i)])
            collisions.append(num)
        host = host_subplot(111, axes_class=AA.Axes)
        plt.subplots_adjust(right=0.75)
        
        par1 = host.twinx()
        par2 = host.twinx()
        offset = 60
        new_fixed_axis = par2.get_grid_helper().new_fixed_axis
        par2.axis["right"] = new_fixed_axis(loc="right",
                                            axes=par2,
                                            offset=(offset, 0))
        par2.axis["right"].toggle(all=True)
        
        host.set_xlabel("Month")
        host.set_ylabel("Collision")
        par1.set_ylabel("Rain")
        par2.set_ylabel("Snow")
        
        xaxis = range(1,self.rangen)
        p1, = host.plot(xaxis, collisions, color = 'r')
        p2, = par1.plot(xaxis, total_rain, color = 'b')
        p3, = par2.plot(xaxis, total_snow, color = 'g')
        
        
        host.axis["left"].label.set_color(p1.get_color())
        par1.axis["right"].label.set_color(p2.get_color())
        par2.axis["right"].label.set_color(p3.get_color())
        
        plt.draw()
        plt.show()

    def weather_collision_merge(self, weather, data):
        weather_collision = weather.merge(data, on= self.label)
        return weather_collision
    
    def plot_weather_collision(self, merged):
        corr = merged.corr()
        fig, ax = plt.subplots(figsize=(10,10))
        cax = ax.matshow(corr)
        plt.xticks(range(len(corr.columns)), corr.columns, rotation='vertical')
        plt.yticks(range(len(corr.columns)), corr.columns)
        fig.colorbar(cax, orientation='vertical')
        plt.show()
            