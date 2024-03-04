
from storage import storeData
from query import processQuery
from proj_configs import (
    PRICE_COLUMN,
    AREA_COLUMN,
    STANDARD_DEVIATION_STAT,
    MIN_STAT,
    AVERAGE_STAT
)
import time


def main():
    while True:
            print("\nOperations:")
            print('1: Store Data.')
            print('2: Run Query')
            print('3: Exit')

            userChoice = input('Choose an operation \n')
            if userChoice == "1":
                storeData()
                print("Data stored successfully \n")
            elif userChoice == "2":
                matricNumber = input('Please input matric number \n')
                print("\nOperations:")
                print('1: Average Price')
                print('2: Min Price')
                print('3: Standard Deviation of Price')
                print('4: Average Area.')
                print('5: Min Area')
                print('6: Standard Deviation of Area')
                choice = input('Please type of statistic (1-6)\n')
                if choice == "1":
                    column = PRICE_COLUMN
                    typeStatistic = AVERAGE_STAT
                elif choice == "2":
                    column = PRICE_COLUMN
                    typeStatistic = MIN_STAT
                elif choice == "3":
                    column = PRICE_COLUMN
                    typeStatistic = STANDARD_DEVIATION_STAT
                elif choice == "4":
                    column = AREA_COLUMN
                    typeStatistic = AVERAGE_STAT
                elif choice == "5":
                    column = AREA_COLUMN
                    typeStatistic = MIN_STAT
                elif choice == "6":
                    column = AREA_COLUMN
                    typeStatistic = STANDARD_DEVIATION_STAT
                
                startTime = time.time()
                processQuery(matricNumber, column, typeStatistic)
                # executeQuery(2022, 2, 4, "ANG MO KIO")
                endTime = time.time()
                elapsedTime = (endTime - startTime) * 1000
                print(f"Time taken for query is {round(elapsedTime, 3)} ms")
                
            else:
                break

    return


if __name__ == '__main__':
    main()