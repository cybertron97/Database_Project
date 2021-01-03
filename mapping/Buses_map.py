
import matplotlib.pyplot as plt
import csv
import sys
from mpl_toolkits.basemap import Basemap
from matplotlib.patches import Polygon

original_stdout = sys.stdout
state_index =[]
column2 =[]
map = Basemap(llcrnrlon=-119,llcrnrlat=22,urcrnrlon=-64,urcrnrlat=49,projection='lcc',lat_1=33,lat_2=45,lon_0=-95)

# load the shapefile, use the name 'states'
map.readshapefile('st99_d00', name='states', drawbounds=True)

# collect the state names from the shapefile attributes so we can
# look up the shape obect for a state by it's name
state_names = []
for shape_dict in map.states_info:
    state_names.append(shape_dict['NAME'])
with open ('Fatalities_1.txt', 'w')as a: 
    with open('final_color_3_Buses.txt', 'r') as f:
        sys.stdout = a
        data = f.readlines()
        #print (data)
        for line in data:
            state_index.append(line.strip().split("\t")[0])
            column2.append(line.strip().split("\t")[1])     
        # print ()
        # print(state_index)
        # print()
        # print(column2)
        for i in range(len(state_index)):
            print(state_index[i])
            print(column2[i])
            ax = plt.gca() # get current axes instance
            seg = map.states[state_names.index(state_index[i])]
            poly = Polygon(seg, facecolor=column2[i],edgecolor=column2[i])
            ax.add_patch(poly)
        plt.title('Buses')
        plt.show() 


sys.stdout = original_stdout

