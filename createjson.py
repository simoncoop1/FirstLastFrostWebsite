import csv

#create a list of all observation locations
#the coordinates
file = '/home/simon/Downloads/uk-daily-temperature-obs/ftp.ceda.ac.uk/badc/ukmo-midas-open/data/uk-daily-temperature-obs/dataset-version-201908/midas-open_uk-daily-temperature-obs_dv-201908_station-metadata.csv'

stations{}

with open(f, newline='') as csvfile:
    areader = csv.reader(csvfile, delimiter=',',doublequote=False)
    started = False #indicated the csv stations has started
    for row in areader:
        if row[1] == 'station_name':
            started = true
            continue
        elif started is False:
            continue
        else:
            stations[row[1]] = {'historic-county':row[],'long':row[],'lat':row[],'elevation':row[],'start-year':row[],'end-year':row[]}


        


#for each site record timestamp, max air temp, min air temp, min grass temp, 
