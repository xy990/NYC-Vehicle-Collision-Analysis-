'''
Created on Dec 15, 2016

@author: twff
'''
import pandas as pd
import numpy as np
from bokeh.io import output_file, show
from bokeh.models import GMapPlot, GMapOptions, ColumnDataSource,HoverTool, Circle, DataRange1d, PanTool, WheelZoomTool, BoxSelectTool# Label

def check_zipcode(zip_code):
    '''
    check whether the zipcode is valid
    '''
    a = [    11219.,  10033.,  11235.,  11216.,  10467.,  11222.,
        11364.,  11201.,  11378.,  10010.,  11691.,  11209.,  10022.,
        11434.,  11236.,  11203.,  10456.,  11001.,  11238.,  11354.,
        10305.,  11207.,  10035.,  10029.,  10023.,  11208.,  11223.,
        10001.,  11413.,  10011.,  11692.,  11204.,  10017.,  11103.,
        11374.,  11385.,  11375.,  11435.,  10453.,  11368.,  10314.,
        11215.,  10459.,  10009.,  10024.,  10031.,  10032.,  11428.,
        11432.,  10468.,  10469.,  10457.,  11415.,  10030.,  10065.,
        11416.,  11419.,  11214.,  11239.,  11229.,  10016.,  10018.,
        10312.,  11234.,  10012.,  11366.,  11221.,  10474.,  10028.,
        11420.,  11101.,  10027.,  10466.,  11217.,  11377.,  10454.,
        10304.,  10013.,  10458.,  10036.,  10002.,  11220.,  10301.,
        10452.,  10308.,  10039.,  11426.,  11233.,  10005.,  11212.,
        10019.,  11433.,  10455.,  10460.,  11231.,  11105.,  11218.,
        11213.,  11356.,  10007.,  10021.,  11423.,  11104.,  10069.,
        10465.,  10473.,  11429.,  11226.,  11230.,  10026.,  11040.,
        10128.,  10306.,  11358.,  11205.,  11421.,  11355.,  11210.,
        11360.,  11372.,  10169.,  11373.,  10463.,  10461.,  11418.,
        10310.,  10038.,  11237.,  11412.,  10040.,  10004.,  11436.,
        10307.,  11379.,  10451.,  11224.,  11211.,  11206.,  10462.,
        11361.,  11225.,  10472.,  10034.,  10470.,  11367.,  11417.,
        10025.,  11249.,  10003.,  10309.,  11363.,  11106.,  11228.,
        10172.,  11427.,  11365.,  10014.,  11370.,  11362.,  11422.,
        11411.,  10075.,  11369.,  11357.,  10037.,  11414.,  11430.,
        11004.,  10475.,  11232.,  11693.,  11694.,  11102.,  10471.,
        10006.,  10020.,  11697.,  10000.,  10123.,  10048.,  10119.,
        10281.,  10280.,  10302.,  10464.,  11109.,  11005.,  10282.,
        10111.,  11359.,  10303.,  10153.,  10803.,  11251.,  10105.,
        11242.,  11695.]
    a = list(map(lambda x: int(x),a))
    if zip_code in a:
        return True
    else:
        return False
        
        
def plot_geomap(zipcode, geomap):

    '''
    Calculate the max_contributing factor, max_vehicle type, number_of_injured and number_of_killed people in a specific area
    '''
    geomap = geomap[pd.notnull(geomap['ZIP CODE'])].copy()
    geomap['ZIP CODE'] = geomap['ZIP CODE'].astype(np.int64)
    analysis = geomap[['NUMBER OF PERSONS INJURED', 'NUMBER OF PERSONS KILLED','ZIP CODE','VEHICLE TYPE CODE 1','CONTRIBUTING FACTOR VEHICLE 1']]
    analysis = analysis.dropna()
    num_injured = analysis.groupby(['ZIP CODE'])['NUMBER OF PERSONS INJURED'].sum().to_frame()
    num_killed = analysis.groupby(['ZIP CODE'])['NUMBER OF PERSONS KILLED'].sum().to_frame()
    injure_killed = pd.concat([num_injured, num_killed], axis = 1)
    injure_killed = injure_killed.reset_index(level = ['ZIP CODE'])
    num_of_injured = int(injure_killed.loc[injure_killed['ZIP CODE'] == zipcode]['NUMBER OF PERSONS INJURED'].values)
    num_of_killed = int(injure_killed.loc[injure_killed['ZIP CODE'] == zipcode]['NUMBER OF PERSONS KILLED'].values)
    
    vehicle = analysis.groupby(['ZIP CODE','VEHICLE TYPE CODE 1']).size()
    vehicle = vehicle.to_frame()
    vehicle.columns = ['car']
    vehicle = vehicle.reset_index(level = ['VEHICLE TYPE CODE 1','ZIP CODE'])
    maxcar = vehicle.loc[vehicle.groupby(['ZIP CODE'])['car'].idxmax()]['VEHICLE TYPE CODE 1']
    vehicle = pd.DataFrame(vehicle, index = maxcar.index)
    vehicle['ZIP CODE'] = vehicle['ZIP CODE'].astype(int)
    maxvehicle = vehicle[vehicle['ZIP CODE'] == zipcode]['VEHICLE TYPE CODE 1'].values
    
    factor = analysis.groupby(['ZIP CODE','CONTRIBUTING FACTOR VEHICLE 1']).size()
    factor = factor.to_frame()
    factor.columns = ['factor']
    factor = factor.reset_index(level = ['CONTRIBUTING FACTOR VEHICLE 1','ZIP CODE'])
    factor = factor[factor['CONTRIBUTING FACTOR VEHICLE 1'] != 'Unspecified']
    maxfactor = factor.loc[factor.groupby(['ZIP CODE'])['factor'].idxmax()]['CONTRIBUTING FACTOR VEHICLE 1']
    factor = pd.DataFrame(factor, index = maxfactor.index)
    factor['ZIP CODE'] = factor['ZIP CODE'].astype(int)
    maxcontributing = factor[factor['ZIP CODE'] == zipcode]['CONTRIBUTING FACTOR VEHICLE 1'].values
    
    geomap_zip = geomap.groupby('ZIP CODE').size().to_frame()
    geomap_zip.columns = ['Counts']
    geomap_zip = geomap_zip.reset_index(level = ['ZIP CODE'])
    total_collisions = int(geomap_zip[geomap_zip['ZIP CODE'] == zipcode]['Counts'].values)
    for a in maxcontributing:
        maxcontributing = a
    for b in maxvehicle:
        maxvehicle = b

                                              
#Plot geomap for the area                                                   
    
    geomap_zip['High'] = geomap_zip['Counts'].map(lambda x: 1 if x >=np.percentile(geomap_zip['Counts'],50) else 0)
  
    if int(geomap_zip[geomap_zip['ZIP CODE']==zipcode]['High'].values)==1:
        print("\nFor zipcode " + str(zipcode) + ": \n" 
            "   There were {} collisions happened in this area for the past year.".format(total_collisions) + '\n'
            "   It was a high probability compared to the other areas."+ '\n'
            "   There were {} people injured, and {} people killed.".format(num_of_injured,num_of_killed) +'\n'
            "   The factor that contributed most to these accidents was: {}".format(maxcontributing)+'\n'
            "   The vehicle type that involved in most of the accidents was: {}".format(str.title(maxvehicle)) +'\n')
    else:
        print("\nFor zipcode " + str(zipcode) + ": \n" 
            "   There were {} collisions happened in this area for the past year.".format(total_collisions) + '\n'
            "   It was a low probability compared to the other areas." + '\n'
            "   There were {} people injured, and {} people killed.".format(num_of_injured,num_of_killed) +'\n'
            "   The factor that contributed most to these accidents was: {}".format(maxcontributing)+'\n'
            "   The vehicle type that involved in most of the accidents was: {}".format(str.title(maxvehicle)))

    collision_lng=[]
    collision_lat=[]
    no_injured = []
    no_killed =[]
    ind = geomap[geomap['ZIP CODE']== zipcode].index.tolist()
    for i in ind:
        collision_lng.append(geomap['LONGITUDE'][i])
        collision_lat.append(geomap['LATITUDE'][i])
        no_injured.append(geomap['NUMBER OF PERSONS INJURED'][i])
        no_killed.append(geomap['NUMBER OF PERSONS KILLED'][i])

    map_options = GMapOptions(lat=np.mean(collision_lat), lng=np.mean(collision_lng), map_type="roadmap", zoom=11)

    plot = GMapPlot(
        plot_width=600, plot_height=800, x_range=DataRange1d(), y_range=DataRange1d(), map_options=map_options)#, title="NYC_Collisions"
    
    source = ColumnDataSource(
        data=dict(
            lat= collision_lat,
            lon= collision_lng,
            collision_no_injured= no_injured,
            collision_no_killed = no_killed
        )
    )
    circle = Circle(x="lon", y="lat", size=2, fill_color="blue", fill_alpha=0.8, line_color=None)
    plot.add_glyph(source, circle)
    plot.add_tools(PanTool(), WheelZoomTool())
    output_file("geomap.html")
    show(plot)

