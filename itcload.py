# Insert this script into the folder to which you want to download the files along with config.cfg, 
# Autoingestion.properties and Autoingestion.class (java file downloadable from Apple)
# Call from the CMD prompt

import glob
import re
import os
import datetime
import ConfigParser
import MySQLdb as mysql

#Configuration file including vendorid, database credentials
config = ConfigParser.RawConfigParser()
config.read('config.cfg')

vendorid = config.get('creds', 'vendorid')
user = config.get('dbcreds', 'user')
password = config.get('dbcreds', 'password')
host = config.get('dbcreds', 'host')
database = config.get('dbcreds', 'database')
mypath = config.get('creds', 'mypath')


#connect to database,get most recent date loaded from itc_daily table
cnx = mysql.connect( host, user, password, database, local_infile=1)
query = ("SELECT MAX(`begin date`) FROM ITC_DAILY")
cursor = cnx.cursor()
cursor.execute(query)
results = cursor.fetchall()
resulttup = results[0]
result = resulttup[0]
max_date_in_table = result.strftime("%Y%m%d")
cursor.connection.autocommit(True)
print max_date_in_table + " is the most recent file in the database" + "\n"


# This is the block of code that creates the list of files already in the folder

def max_report_date():
    current_daily = [f for f in glob.glob('S_D_*') if '.gz' not in f]
    current_daily_dates = []
    for daily in current_daily:
        daily_file = os.path.split(daily)[1]
        daily_file_date = daily_file[13:len(daily_file)-4]
        current_daily_dates.append(daily_file_date)
    max_file_date = max(current_daily_dates)
    return max_file_date

maxreportdate = max_report_date()

#now what days do you need to download, returns a list of the itunes style filedate distinction
def days_to_download():
    today = datetime.datetime.now()
    days_datetime = today - datetime.datetime.strptime(maxreportdate, "%Y%m%d")
    days_string = str(days_datetime)
    days_slice = days_string[:days_string.find(' ')]
    days = int(days_slice) -1
    days_to_dwn = []
    if days == 0:
        print "There are no more reports to download yet, %s is the most recent report available" % maxreportdate
    while days > 0:
        day_to_add = today - datetime.timedelta(days)
        days_to_dwn.append(day_to_add.strftime("%Y%m%d"))
        days -= 1
    return days_to_dwn

#need to put new line in FRONT of text
print '...downloading and processing files from Apple' + "\n"

#this actually downloads the files according to the list you just created (python wrapper for java .class file in folder provided by apple)
def report_downloader(days_to_download):
    for d in days_to_download:
        cmd = ["java", "Autoingestion", "autoingestion.properties", vendorid, "Sales", "Daily", "Summary", d]
        cmdstring = ' '.join(cmd)
        os.popen(cmdstring)

report_downloader(days_to_download())

#Unzip the gz files
files = glob.glob('S_D_*')
def unzip_files(files):
    for f in files:
        if f.find('.gz') != -1:
            zipcmd = ["gzip", "-d", f ]
            zipcmdstring = ' '.join(zipcmd)
            os.popen(zipcmdstring)
unzip_files(files)
files = glob.glob('S_D_' + vendorid + '*.txt')


#determine which of the files in the folder to upload to the database
files_to_load = []
def add_files():
    for f in files:
        z = f[13:len(f)-4]
        y = ("S_D_%s_%s") % (vendorid, z)
        if z > max_date_in_table:
            files_to_load.append(y)
    return files_to_load
add_files()

#insert file in itc_daily database
def load_itc_daily(loadfile):
    filename = "%s%s.txt" % (mypath,loadfile)
    droptable1 = ("drop table if exists newitcdaily2;")
    # creates a table (could be temporary) for the data to be loaded into -- to handle dates
    # could refactor this to say for all text files, load into this table first THEN load into DB (non-priority)
    createtable = ("""create table newitcdaily2 (
            Provider varchar(150),
            `Provider Country` varchar(150),
            SKU varchar(150),
            Developer varchar(150),
            Title varchar(150),
            Version varchar(150),
            `Product Type Identifier` varchar(150),
            Units varchar(150),
            `Developer Proceeds` varchar(150),
            `Begin Date` varchar(64),
            `End Date` varchar(64),
            `Customer Currency` varchar(150),
            `Country Code` varchar(150),
            `Currency of Proceeds` varchar(150),
            `Apple Identifier` varchar(150),
            `Customer Price` varchar(150),
            `Promo Code` varchar(150),
            `Parent Identifier` varchar(150),
            `Subscription` varchar(150),
            `Period` varchar(150));""")

    load_query = ("LOAD DATA LOCAL INFILE '%s' INTO TABLE newitcdaily2 IGNORE 1 LINES;" %(filename))
    insert_query = ("""insert into ITC_Daily
            SELECT
            Provider,
            `Provider Country`,
            SKU,
            Developer,
            Title,
            Version,
            `Product Type Identifier`,
            Units,
            `Developer Proceeds`,
            CONCAT(right(`begin date`,4),'-',left(`begin date`,2),'-',mid(`begin date`,4,2)),
            CONCAT(right(`end date`,4),'-',left(`end date`,2),'-',mid(`end date`,4,2)),
            `Customer Currency`,
            `Country Code`,
            `Currency of Proceeds`,
            `Apple Identifier`,
            `Customer Price`,
            `Promo Code`,
            `Parent Identifier`,
            `Subscription`,
            `Period`
            from newitcdaily2;""")
    cursor.execute(droptable1)
    cnx.commit()
    cursor.execute(createtable)
    cnx.commit()
    cursor.execute(load_query)
    cnx.commit()
    cursor.execute(insert_query)
    cnx.commit()

#for files to be uploaded, execute upload function
def load_to_db():
    for f in files_to_load:
        load_itc_daily(f)


load_to_db()

#final message, what has been added to the table

# maybe put a line count in here and do line count for files to load, line count for new files in DB for Match check
#something like WC files - n(accounting for title lines) = count(*) files added greater than original maxdate
cursor.execute("SELECT MAX(`BEGIN DATE`) from ITC_DAILY")
final_results_fetch = (cursor.fetchall())
final_results_tup = final_results_fetch[0]
final_results = final_results_tup[0]
string_result = final_results.strftime("%m/%d/%Y")
print "...these files have been added:"
for f in files_to_load:
    print f
print "Your data has been uploaded through %s" % (string_result)
cursor.close()
cnx.close()


