import csv

#create a list of all observation locations
#the coordinates
f = '/home/simon/Downloads/uk-daily-temperature-obs/ftp.ceda.ac.uk/badc/ukmo-midas-open/data/uk-daily-temperature-obs/dataset-version-201908/midas-open_uk-daily-temperature-obs_dv-201908_station-metadata.csv'

stations = {}

with open(f, newline='') as csvfile:
    areader = csv.reader(csvfile, delimiter=',',doublequote=False)
    started = False #indicated the csv stations has started
    for row in areader:
        if len(row) == 9  and row[1] == 'station_name' and row[0] == 'src_id':
            started = True
            continue
        elif row[0] == 'end data':
            continue
        elif started is False:
            continue
        else:
            print(row[0])
            stations[row[1]] = {'historic-county':row[3],'long':row[5],'lat':row[4],'elevation':row[6],'start-year':row[7],'end-year':row[8]}

from pymongo import MongoClient

class Connect(object):
        @staticmethod    
        def get_connection():
            hostlist = 'localhost'
            database = 'test'
            return MongoClient(f"mongodb://{hostlist}/{database}")

connection = Connect.get_connection()
        
print(stations)

#for each site record timestamp, max air temp, min air temp, min grass temp, 
