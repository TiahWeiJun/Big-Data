from proj_configs import (
    STORAGE_DIRECTORY,
    TOWN_MAPPING,
    YEAR_MAPPING,
    MONTH_MAPPING,
    VECTOR_SIZE,
    NEW_RELEVANT_COLUMNS,
    OUTPUT_DIRECTORY,
    PRICE_COLUMN,
    AREA_COLUMN,
    MIN_STAT,
    AVERAGE_STAT,
    STANDARD_DEVIATION_STAT
)
from utils import (
    getMonthCompressionMap,
    getYearCompressionMap
)
import os
import csv
import math
import threading

# Contain basic stats like total min, total sum, total count for a certain query
# Purpose is 1) store the basic stats to calculate standard deviation  2) for fast access again for repeating queries
# Format is like "{year}-{startMonth}-{town}-{col}": {"min": x, "avg":y, "count": z, "sd": "a"}
queryStatsCache = {}

lock = threading.Lock()

def processQuery(matricNumber, resultColumn, typeStatistic):

    # query parameters
    year = YEAR_MAPPING[int(matricNumber[-2])]
    startMonth = MONTH_MAPPING[int(matricNumber[-3])]
    endMonth = startMonth + 2
    town = TOWN_MAPPING[int(matricNumber[-4])]

    # cache key for this query
    cacheKey = f"{year}-{startMonth}-{town}-{resultColumn}" 

    # statistic value (min, avg, sd) to be written to output
    statValue = 0 

    # calculating SD is special, need two rounds, first round to get basic stats like total count and sum, second round to find actual SD
    if typeStatistic == STANDARD_DEVIATION_STAT:
        if cacheKey not in queryStatsCache:
            # execute first round processing
            executeQuery(year, startMonth, endMonth, town, resultColumn, MIN_STAT)

        statDict = queryStatsCache[cacheKey]
        # if already have sd in cache then don need calculate
        if "sd" in statDict:
            statValue = statDict["sd"]
        else:
            # second round of processing to calculate sd
            statValue = executeQuery(year, startMonth, endMonth,town, resultColumn, typeStatistic)

    # min or area statistic
    else: 
        if cacheKey in queryStatsCache:
            statDict = queryStatsCache[cacheKey]
            if typeStatistic == MIN_STAT:
                statValue = statDict["min"]
            else:
                if statDict["count"] == 0:
                    statValue = 0
                else:
                    statValue = statDict["sum"]/statDict["count"]
        else:
            # execute query
            statValue = executeQuery(year, startMonth, endMonth,town, resultColumn, typeStatistic)

    # write to file output
    writeResultToFile(matricNumber, resultColumn, typeStatistic, year, startMonth, town, statValue)
    
    


def writeResultToFile(matricNumber, resultColumn, typeStatistic, year, startMonth, town, statValue):
    outputFileName = f"{OUTPUT_DIRECTORY}/ScanResult_{matricNumber}.csv"
    fileExists = os.path.isfile(outputFileName)
    with open(outputFileName, 'a' if fileExists else 'w', newline='') as outputFile:
        writer = csv.writer(outputFile)

        if not fileExists:
            writer.writerow(["Year", "Month", "Town", "Category", "Value"])

        if resultColumn == PRICE_COLUMN:
            colName = "Price"
        else:
            colName = "Area"

        if typeStatistic == MIN_STAT:
            category = f"Min {colName}"
        elif typeStatistic == AVERAGE_STAT:
            category = f"Average {colName}"
        else:
            category = f"Standard Deviation of {colName}"

        writer.writerow([year, startMonth, town, category, round(statValue, 2)])


def executeQuery(queryYear, queryStartMonth, queryEndMonth, queryTown, resultColumn, typeStatistic):
    
    # take in zone map for the year and find relevant batches
    batchList = []
    with open(f"{STORAGE_DIRECTORY}/zone-map-year.csv", 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        
        for row in reader:
            batchNum, minYear, maxYear = row
            if int(minYear) <= queryYear <= int(maxYear):
                batchList.append(int(batchNum))

    # Stats for result column (price/area)
    totalStatDict = {
        "min": float('inf'),
        "count": 0,
        "sum": 0
    }
    # to calculate sd
    sumSquaredDiff = [0]
    cacheKey = f"{queryYear}-{queryStartMonth}-{queryTown}-{resultColumn}"
    
    # do batch by batch
    yearBatchList = []
    openedFiles = {}
    for batch in batchList:
        # open relevant files first
        for col in NEW_RELEVANT_COLUMNS:
            # only take the result column file we want
            if resultColumn == PRICE_COLUMN and col == AREA_COLUMN:
                continue
            if resultColumn == AREA_COLUMN and col == PRICE_COLUMN:
                continue

            filePath = os.path.join(STORAGE_DIRECTORY, f"{col}-{batch}.csv")
            openedFiles[f"{col}-{batch}"] = open(filePath, 'r', newline='')

        yearReader = csv.reader(openedFiles[f"year-{batch}"])
        for row in yearReader:
            yearBatchList.append(row[0])

        # do vector by vector
        curYearIndex = 0
        iterations = -1
        threads = []
        while curYearIndex < len(yearBatchList):
            iterations += 1
            yearVector = yearBatchList[curYearIndex: curYearIndex + VECTOR_SIZE]
            curYearIndex += VECTOR_SIZE

            # run multi-threading for each vector in one batch
            # thread = threading.Thread(target=vectorizedQueryProcessing, args=(yearVector, iterations, openedFiles, batch,queryYear, queryStartMonth, queryEndMonth, queryTown, resultColumn, typeStatistic, cacheKey, totalStatDict, sumSquaredDiff ))
            # thread.start()
            # threads.append(thread)
            vectorizedQueryProcessing(yearVector, iterations, openedFiles, batch,queryYear, queryStartMonth, queryEndMonth, queryTown, resultColumn, typeStatistic, cacheKey, totalStatDict, sumSquaredDiff )
        
        # wait for all threads to finish
        # for thread in threads:
        #     thread.join()

         # Close all opened batch files
        for file in openedFiles.values():
            file.close()
    
    # if is sd, calculate sd and store in cache
    if typeStatistic == STANDARD_DEVIATION_STAT:
        cachedTotalCount = queryStatsCache[cacheKey]["count"]
        standardDeviation = 0
        if cachedTotalCount != 0:
            variance = sumSquaredDiff[0] / (cachedTotalCount - 1)
            standardDeviation = math.sqrt(variance)
            queryStatsCache[cacheKey]["sd"] = standardDeviation

        return standardDeviation

    # if is min/avg, store values in cache and return result
    queryStatsCache[cacheKey] = totalStatDict
    if typeStatistic == MIN_STAT:
        return totalStatDict['min']
    else:
        totalResultAvg = 0
        if totalStatDict['count'] != 0:
            totalResultAvg = totalStatDict['sum'] / totalStatDict['count']
        
        return totalResultAvg
    
    

def vectorizedQueryProcessing(yearVector, iterations, openedFiles, batch,queryYear, queryStartMonth, queryEndMonth, queryTown, resultColumn, typeStatistic, cacheKey, totalStatDict, sumSquaredDiff):
    # handle year processing
    vectorPositionList = vectorizedYearProcessing(yearVector, queryYear, iterations)

    if len(vectorPositionList) == 0: 
        return

    # handle month processing
    vectorPositionList = vectorizedMonthProcessing(openedFiles[f"month-{batch}"], queryStartMonth, queryEndMonth, vectorPositionList)

    # check if vectorPosition list is empty
    if len(vectorPositionList) == 0: 
        return

    # handle town processing
    vectorPositionList = vectorizedTownProcessing(openedFiles[f"town-{batch}"], queryTown, vectorPositionList)
    
    # check if vectorPosition list is empty
    if len(vectorPositionList) == 0:
        return
    
    # get result vector based on filtered position list
    resultVector = getResultVectors(openedFiles[f"{resultColumn}-{batch}"], vectorPositionList)

    
    with lock:
        # calculate stats for min and average 
        if (typeStatistic != STANDARD_DEVIATION_STAT):
            sumResult, countResult, minResult = calculateStats(resultVector)
            totalStatDict["sum"] += sumResult
            totalStatDict["count"] += countResult
            totalStatDict["min"] = min(totalStatDict["min"], minResult)
        # calculate stats for sd
        else:
            # get total average and count from cache
            statDict = queryStatsCache[cacheKey]
            cachedTotalAverage = statDict["sum"]/statDict["count"]
            for val in resultVector:
                sumSquaredDiff[0] += (val - cachedTotalAverage) ** 2


def vectorizedYearProcessing(yearVector, queryYear, iterations):
    # get year compression mapping
    yearCompressionMap = getYearCompressionMap()

    vectorPositionList = []
    # search for year using binary search because its sorted
    startPosition = binary_search_first_occurrence(yearVector, queryYear, yearCompressionMap)

    # if cannot find, means not in this vector
    if startPosition == -1:
        return vectorPositionList
    
    # create vector position list that correspond to the year
    while startPosition < len(yearVector) and yearCompressionMap[yearVector[startPosition]] == queryYear:
        vectorPositionList.append(startPosition + (iterations*VECTOR_SIZE))
        startPosition += 1

    return vectorPositionList


def vectorizedMonthProcessing(monthBatchFile, queryStartMonth, queryEndMonth, vectorPositionList):
    # get year compression mapping
    monthCompressionMap = getMonthCompressionMap()

    # get month vector based on positionList
    monthVector = []
    with lock:
        monthBatchFile.seek(0)  
        monthReader = csv.reader(monthBatchFile)
        next(monthReader)
        curPosIndex = 0
        for pos, row in enumerate(monthReader):
            if vectorPositionList[curPosIndex] == pos:
                monthVector.append(row[0])
                curPosIndex += 1
                if curPosIndex >= len(vectorPositionList):
                    break
    
    # create new vector position list that correspond to the month
    newVectorPositionList = []
    for i, monthVal in enumerate(monthVector):
        if queryStartMonth <= monthCompressionMap[monthVal] <= queryEndMonth:
            newVectorPositionList.append(vectorPositionList[i])

    return newVectorPositionList


def vectorizedTownProcessing(townBatchFile, queryTown, vectorPositionList):
    townVector = []
    with lock:
        townBatchFile.seek(0)  
        townReader = csv.reader(townBatchFile)
        next(townReader)
        curPosIndex = 0
        for pos, row in enumerate(townReader):
            if vectorPositionList[curPosIndex] == pos:
                townVector.append(row[0])
                curPosIndex += 1
                if curPosIndex >= len(vectorPositionList):
                    break

    # create new vector position list that correspond to the town
    newVectorPositionList = []
    for i, townVal in enumerate(townVector):
        if townVal == queryTown:
            newVectorPositionList.append(vectorPositionList[i])
    
    return newVectorPositionList

    
def getResultVectors(resultFile, vectorPositionList):
    
    resultVector = []
    with lock:
        resultFile.seek(0)  
        resultReader = csv.reader(resultFile)
        next(resultReader)
        curPosIndex = 0
        for pos, row in enumerate(resultReader):
            if vectorPositionList[curPosIndex] == pos:
                resultVector.append(int(row[0]))
                curPosIndex += 1
                if curPosIndex >= len(vectorPositionList):
                    break

        return resultVector


def calculateStats(data):
    sumData = 0
    countData = 0
    minData = float('inf')

    for val in data:
        sumData += val
        countData += 1
        minData = min(minData, val)

    return sumData, countData, minData

   


        
def binary_search_first_occurrence(arr, target, yearCompressionMap):
    left, right = 0, len(arr) - 1
    result = -1

    while left <= right:
        mid = left + (right - left) // 2

        val = yearCompressionMap[arr[mid]]
        if val == target:
            result = mid
            right = mid - 1  
        elif val < target:
            left = mid + 1
        else:
            right = mid - 1

    return result


