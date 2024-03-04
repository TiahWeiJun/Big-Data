from proj_configs import (
    STORAGE_DIRECTORY,
)

import threading
import csv

lock = threading.Lock()

def getYearCompressionMap():
    yearCompressionMap = {}
    with lock:
        with open(f"{STORAGE_DIRECTORY}/year-compression.csv", 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                yearCompressionMap[row[0]] = int(row[1])
                
        return yearCompressionMap

def getMonthCompressionMap():
    monthCompressionMap = {}
    with lock:
        with open(f"{STORAGE_DIRECTORY}/month-compression.csv", 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                monthCompressionMap[row[0]] = int(row[1])
                
        return monthCompressionMap