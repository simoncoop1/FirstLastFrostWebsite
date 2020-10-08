import csv
from pymongo import MongoClient
import os
from bson.son import SON


class Connect(object):
        @staticmethod    
        def get_connection():
            hostlist = 'localhost'
            database = 'test'
            return MongoClient(f"mongodb://{hostlist}/{database}")

#create a list of all observation locations
#the coordinates
def stations():
    f = '/home/simon/Downloads/uk-daily-temperature-obs/ftp.ceda.ac.uk/badc/ukmo-midas-open/data/uk-daily-temperature-obs/dataset-version-201908/midas-open_uk-daily-temperature-obs_dv-201908_station-metadata.csv'

    stations = []

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
                key = row[1].replace('.',',')
                stations.append({key:{'historic-county':row[3],'long':row[5],'lat':row[4],'elevation':row[6],'start-year':row[7],'end-year':row[8]}})


    c = MongoClient()
    db = c.test

    db.stations.insert_many(stations)
            
    print(stations)


#for each site record timestamp, max air temp, min air temp, min grass temp, 

root = '/home/simon/Downloads/uk-daily-temperature-obs/ftp.ceda.ac.uk/badc/ukmo-midas-open/data/uk-daily-temperature-obs/'
filesIncPath = []
for path, subdirs, files in os.walk(root):
    for name in files:
        if '/qc-version-0' in path:
            continue
        if name.endswith('capability.csv') or name.endswith('_change_log.txt') or name.endswith('README_catalogue_and_licence.txt') or name.startswith('.') or name.endswith('_station-metadata.csv'):
            continue
        #print(os.path.join(path, name))
        filesIncPath.append(os.path.join(path, name))


obsv = []

for f in filesIncPath:
    with open(f, newline='') as csvfile:
        areader = csv.reader(csvfile, delimiter=',',doublequote=False)
        dataMarkerFound = False
        dataColumnHeaderFound = False
        station = {}
        print(f)
        for row in areader:            
            if row[0] == 'data':
                dataMarkerFound = True
                continue
            elif row[0] == 'observation_station':
                station['observation_station'] = row[2]
                station['data'] = []
                continue
            elif row[0] == 'end data':#end of data reached
                continue
            elif row[0] == 'ob_end_time' and dataMarkerFound:
                dataColumnHeaderFound = True
                continue
            elif dataMarkerFound and dataColumnHeaderFound:
                station['data'].append({'ob_end_time':row[0],'ob_hour_count':row[3],'max_air_temp':row[8],'min_air_temp':row[9],'min_grss_temp':row[10],'min_conc_temp':row[11]})
                
                                    
        obsv.append(station)
        #print(obsv)
        #break #temp

c = MongoClient()
db = c.test

db.observations.insert_many(obsv)
    





