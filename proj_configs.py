DATA_FILE = 'data/ResalePricesSingapore.csv'

MAX_FILE_LINE = 50000
VECTOR_SIZE = 50

STORAGE_DIRECTORY = 'storage'
OUTPUT_DIRECTORY = 'output'

AREA_COLUMN = "floor_area_sqm"
PRICE_COLUMN = "resale_price"
TOWN_COLUMN = "town"
MONTH_COLUMN = "month"
YEAR_COLUMN = "year"

AVERAGE_STAT = "average"
MIN_STAT = "min"
STANDARD_DEVIATION_STAT = "sd"

RELEVANT_COLUMNS = {"month", "town", "resale_price", "floor_area_sqm"}
NEW_RELEVANT_COLUMNS = [YEAR_COLUMN, MONTH_COLUMN, TOWN_COLUMN, PRICE_COLUMN, AREA_COLUMN]

TOWN_MAPPING = ["ANG MO KIO", "BEDOK", "BUKIT BATOK", "CLEMENTI", "CHOA CHU KANG", "HOUGANG", "JURONG WEST", "PUNGGOL", "WODDLANDS", "YISHUN"]
YEAR_MAPPING = [2020, 2021, 2022, 2023, 2014, 2015, 2016, 2017, 2018, 2019]
MONTH_MAPPING = [10, 1, 2, 3, 4, 5, 6, 7, 8, 9]



