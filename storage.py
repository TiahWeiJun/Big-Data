from proj_configs import (
    DATA_FILE,
    MAX_FILE_LINE,
    STORAGE_DIRECTORY,
    RELEVANT_COLUMNS,
)
from utils import (
    getYearCompressionMap
)

import os
import csv

def createYearZoneMap():
    batch = 0

    # get compression map
    yearCompressionMap = getYearCompressionMap()

    # create file for zone map coresponding to year file
    filePath = os.path.join(STORAGE_DIRECTORY, f"zone-map-year.csv")
    zoneMapFile = open(filePath, 'w', newline='')
    writer = csv.writer(zoneMapFile)

    while os.path.exists(f"{STORAGE_DIRECTORY}/year-{batch}.csv"):
        yearFilePath = f"{STORAGE_DIRECTORY}/year-{batch}.csv"
        minYearZone = float('inf')
        maxYearZone = 0

        with open(yearFilePath, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                yearVal = yearCompressionMap[row[0]]
                minYearZone = min(minYearZone, yearVal)
                maxYearZone = max(maxYearZone, yearVal)

        writer.writerow([batch, minYearZone,  maxYearZone])
        batch += 1

    
    zoneMapFile.close()
    return
    

def storeData():

    os.makedirs(STORAGE_DIRECTORY, exist_ok=True)

    # create compression mapping for year and month
    compressYearMapping = createYearCompression()
    compressMonthMapping = createMonthCompression()

    

    # Open the CSV file for reading
    with open(DATA_FILE, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Get the column names
        columns = reader.fieldnames
        output_files = {}

        # Create an output file for each column
        output_files = {}
        for column in columns:
            if column not in RELEVANT_COLUMNS:
                continue
            
            batch = 0
            lineCount = 0
            if column == "month":
                # create col for year
                output_file_path = os.path.join(STORAGE_DIRECTORY, f"year-{batch}.csv")
                output_files[f"year-{batch}"] = open(output_file_path, 'w', newline='')

                # create col for month
                output_file_path = os.path.join(STORAGE_DIRECTORY, f"month-{batch}.csv")
                output_files[f"month-{batch}"] = open(output_file_path, 'w', newline='')
            else:

                output_file_path = os.path.join(STORAGE_DIRECTORY, f"{column}-{batch}.csv")
                output_files[f"{column}-{batch}"] = open(output_file_path, 'w', newline='')

            # reset reader
            csvfile.seek(0)  
            next(reader)
            for row in reader:
                if lineCount == MAX_FILE_LINE: 

                    # create new file
                    batch += 1
                    if column == "month":
                        output_file_path = os.path.join(STORAGE_DIRECTORY, f"year-{batch}.csv")
                        output_files[f"year-{batch}"] = open(output_file_path, 'w', newline='')

                        output_file_path = os.path.join(STORAGE_DIRECTORY, f"month-{batch}.csv")
                        output_files[f"month-{batch}"] = open(output_file_path, 'w', newline='')
                    else:
                        output_file_path = os.path.join(STORAGE_DIRECTORY, f"{column}-{batch}.csv")
                        output_files[f"{column}-{batch}"] = open(output_file_path, 'w', newline='')


                    lineCount = 0
                
                if column == "month":
                    year = row[column].split("-")[0]
                    month = row[column].split("-")[1]

                    # write for year
                    writer = csv.writer(output_files[f"year-{batch}"])
                    writer.writerow([compressYearMapping[int(year)]])

                    # write for month
                    writer = csv.writer(output_files[f"month-{batch}"])
                    writer.writerow([compressMonthMapping[int(month)]])

                else:
                    # write to file
                    writer = csv.writer(output_files[f"{column}-{batch}"])
                    writer.writerow([row[column]])

                lineCount += 1

    # Close all output files
    for file in output_files.values():
        file.close()

     # create zone map for year
    createYearZoneMap()
            


def createYearCompression():
    mappingList = ['0000', '0001', '0010', '0011', '0100', '0101', '0110', '0111', '1000', '1001', '1010']
    mappingDict = {}
    with open(f"{STORAGE_DIRECTORY}/year-compression.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        startYear = 2014
        for val in mappingList:
            writer.writerow([val, startYear])
            mappingDict[startYear] = val
            startYear += 1

    return mappingDict


def createMonthCompression():
    mappingList = ['0000', '0001', '0010', '0011', '0100', '0101', '0110', '0111', '1000', '1001', '1010', '1011']
    mappingDict = {}
    with open(f"{STORAGE_DIRECTORY}/month-compression.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        startMonth = 1
        for val in mappingList:
            writer.writerow([val, startMonth])
            mappingDict[startMonth] = val
            startMonth += 1

    return mappingDict
