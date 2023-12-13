# Jon Person
# Assignment: Housing Project
'''
Program Location (5 points)
Create a PyCharm project called Housing project. The main program should be called main.py

Required File (5 points)
Within your PyCharm project, create a file called files.py. This file will be imported from main.py by:
- "from files import *"
Within the files.py file, there will be three assignments:
- housingFile = "</path/to/file>"
- incomeFile = "</path/to/file>"
- zipFile = "</path/to/file>"

Program Execution (5 points)
I will run your program within PyCharm, executing only the main.py file.
Output (10 points)
When I run your program I expect to see only the following:
-Beginning import
-Cleaning Housing File data
-100 records imported into the database
-Cleaning Income File data
-100 records imported into the database
-Cleaning ZIP File data
-100 records imported into the database
-Import completed
-Beginning validation
-Total Rooms: 111
-For locations with more than 111 rooms, there are a total of
-222 bedrooms.
-ZIP Code: 33333
-The median household income for ZIP code 33333 is 444,444.
-Program exiting.

Correct Data (25 points)
I will look at the numbers from NOTES section above. You will lose points for each of these
numbers where the actual answer is incorrect.

Errors (-3 points each)
For every uncaught exception, spelling, grammar, and punctuation error you make will accrue a
3 point deduction. This only applies to program output.
'''

# First, import all the necessary packages and the information from the separate files
import random
import pandas as pd
import pymysql.cursors

# Import your files from "files.py"
from files import *

# Import your credentials from credentials.py; I'm still using pyautogui for masking the password
from credentials import *

# Originally, I defined all my functions in a separate .py file to keep main.py clean.
# I was unable to execute them from main.py

# Rename each of the files from "files.py" as a shorter, more manageable variable.
# Use the read_csv function in pandas to read the files
house = pd.read_csv(housingFile)
income = pd.read_csv(incomeFile)
cityCounty = pd.read_csv(zipFile)

# Use this modification so that you can see all columns
pd.set_option("display.max.columns", None)

# If you don't make this modification to settings in pandas, then you get a warning message for each entry before the
# program even starts. Not fun.
pd.options.mode.chained_assignment = None

# This merges the two datasets together based on their unique identifier: guid
dataMerge = pd.merge(house, income)

# Merge the previous and last dataset together
dataComp = pd.merge(dataMerge, cityCounty)

# Remove all rows that contained four letters in guid
dataMerge = dataComp[dataComp['guid'].map(len) != 4]

# This reviews each column to check for corrupted values and change the value
for each in dataMerge.index:
    dataMerge.at[each, 'housing_median_age'] = random.randint(10, 50) if \
        (len(dataMerge.at[each, 'housing_median_age']) == 4) else dataMerge.at[each, 'housing_median_age']
    dataMerge.at[each, 'total_rooms'] = random.randint(1000, 2000) if (len(dataMerge.at[each, 'total_rooms']) ==
                                                                       4) else dataMerge.at[each, 'total_rooms']
    dataMerge.at[each, 'total_bedrooms'] = random.randint(1000, 2000) if \
        (len(dataMerge.at[each, 'total_bedrooms']) == 4) else dataMerge.at[each, 'total_bedrooms']
    dataMerge.at[each, 'population'] = random.randint(5000, 10000) if (len(dataMerge.at[each, 'population']) ==
                                                                   4) else dataMerge.at[each, 'population']
    dataMerge.at[each, 'households'] = random.randint(500, 2500) if (len(dataMerge.at[each, 'households']) ==
                                                                 4) else dataMerge.at[each, 'households']
    dataMerge.at[each, 'median_house_value'] = random.randint(100000, 250000) if (
            len(dataMerge.at[each, 'median_house_value']) == 4) else dataMerge.at[each, 'median_house_value']
    dataMerge.at[each, 'median_income'] = random.randint(100000, 750000) \
        if len(dataMerge.at[each, 'median_income']) == 4 else dataMerge.at[each, 'median_income']

# Now, we turn it back into a dataframe so that Pandas can use it.
dataComp = pd.DataFrame(data=dataMerge)

# Now that it's a dataframe again, sort it by the 'state' column
dataComp.sort_values(by=['state'], inplace=True)

# Set this option so that I may see all columns and rows for examination
pd.set_option("display.max_rows", None, "display.max_columns", None)

# After your sort, reset your index. It won't run if you don't.
dataComp = dataComp.reset_index(drop=True)

# This goes throught data value by value, finds the state that was next to the value, and
# grabs the first number of the previous zip code
for each in dataComp.index:
    dataComp.at[each, 'zip_code'] = dataComp.at[each-1, 'zip_code'][-5] + "0000" if len(dataComp.at[each,
    'zip_code']) == 4 else dataComp.at[each, 'zip_code']

# Rename the column so that it can be put into the database in MySQL
dataComp.rename(columns={'housing_median_age': 'median_age'}, inplace = True)

# These establish the specific columns of the data and their respective numeric entries
dataComp['zip_code'] = pd.to_numeric(dataComp['zip_code'])
dataComp['median_age'] = pd.to_numeric(dataComp['median_age'])
dataComp['total_rooms'] = pd.to_numeric(dataComp['total_rooms'])
dataComp['total_bedrooms'] = pd.to_numeric(dataComp['total_bedrooms'])
dataComp['population'] = pd.to_numeric(dataComp['population'])
dataComp['median_house_value'] = pd.to_numeric(dataComp['median_house_value'])
dataComp['households'] = pd.to_numeric(dataComp['households'])
dataComp['median_income'] = pd.to_numeric(dataComp['median_income'])

# This measures the amount of data you have and removes the - from guid
for each in range(0,len(dataComp.index)):
    dataComp.guid[each] = dataComp.guid[each].replace('-', '')

# This reorders the columns to meet the criteria in the Housing Project specifications
dataComp = dataComp[['guid', 'zip_code', 'city', 'state', 'county', 'median_age', 'total_rooms', 'total_bedrooms',
                     'population', 'households', 'median_income', 'median_house_value']]

# This connects us to the database using info in credentials.py
try:
    myConnection = pymysql.connect(host=host,
                                   user=user,
                                   password=password,
                                   db='housing_project',
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)

# Allow for exceptions in connections to database
except Exception:
    print(f"Unable to establish connection to database. Please check your login credentials and try again.")
    print()
    exit()

# This function uploads data to sql database that we created
def uploadData():
    # sqlUpload defines the SQL statement for uploading the data
    for each, row in dataComp.iterrows():
        sqlUpload = """ 
            INSERT INTO housing (guid, zip_code, city, state, county, median_age, total_rooms, total_bedrooms, 
            population, households, median_income, median_house_value) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
    # Execute sqlUpload and update the database
        try:
            tempVar = tuple(row)
            cursor.execute(sqlUpload, tempVar)

        except Exception as e:
            print(f"{e}")


# Use your connection established above and upload data to the dataframe with the above function
try:
    with myConnection.cursor() as cursor:
        uploadData()
        myConnection.commit()
except Exception:
    print(f"Unable to establish connection to database. Please check your login credentials and try again.")
    print()

# Displays the required messages and number of records being imported for each of the three files.
print(f"Beginning import")
print(f"Cleaning Housing File data")
print(f"{len(dataComp)} records imported into the database")
print(f"Cleaning Income File data")
print(f"{len(dataComp)} records imported into the database")
print(f"Cleaning ZIP File data")
print(f"{len(dataComp)} records imported into the database")
print("Import completed")
print("Beginning validation")

# Requests the user input for number of rooms and ensures the input is valid. 'rooms' will be
# used in the roomNumber function and the input must be an integer
while True:
    rooms = input("Total Rooms: ")
    if rooms.isdigit() == True:
        if int(rooms) < dataComp['total_rooms'].max() | int(rooms) >= 0:
            break
    else:
        print("Invalid input. Remember, it must be a whole integer greater than/equal to 0. Please try again.")

# Using the input from 'rooms', we get a sum of bedrooms
def roomNumber(rooms):
    # This SQL statement will give the sum total of the number of bedrooms
        sqlRoom = """ 
            select sum(total_bedrooms) from housing where (total_rooms) > %s;
            """
        cursor.execute(sqlRoom, rooms)
        first_row = cursor.fetchone()
        print(f"For locations with more than {rooms} rooms, there are a total of {first_row['sum(total_bedrooms)']} bedrooms.")

# Now execute the query for number of rooms using the connection to the database
try:
    with myConnection.cursor() as cursor:
        roomNumber(rooms)
        myConnection.commit()
except Exception:
    print(f"Unable to establish connection to database. Please check your login credentials and try again.")
    print()

# User provides an input of zip code and stops if the user provides and incorrect input
while True:
    zip = input("Zip Code: ")
    if str(zip) in str(dataComp.zip_code):
         break
    else:
        print("Entry not found, or is invalid as a zip code. Please try again. ")

# This function will display the zip code and median income based on the user input
def zipIncome(zip):
    # This is the SQL code to select median_income based upon the value of zip_code
        sqlZipIncome = """ 
            select median_income from housing where zip_code = %s;
            """
        cursor.execute(sqlZipIncome, zip)
        first_row = cursor.fetchone()
        placeHolder = first_row['median_income']
        medianIncome = "{:,}".format(placeHolder)
        print(f"The median household income for ZIP code {zip} is {medianIncome}.")

# Execute our query for zip code and median income
try:
    with myConnection.cursor() as cursor:
        zipIncome(zip)
        myConnection.commit()
except Exception:
    print(f"Unable to establish connection to database. Please check your login credentials and try again.")
    print()

# Close connection when you are done
finally:
    myConnection.close()
    print()

print("Program exiting.")
