import pickle
import logging
import datetime
import pandas as pd
import pyodbc
import numpy as np
import os
import shutil
import glob

logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename='C:\\Simon\\DailySales\\Logs\\Sales_{}.log'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
pd.set_option('display.float_format', lambda x: '%.5f' % x)

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.ERROR)

# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)
logging.info('Process has started')

##Load/write
ldap_detl = {"User": "SVC_SSIS_Prod", "Password": "1%lyljW%,uBafqz"}
pickle.dump( ldap_detl, open( "save.p", "wb" ))

##Load user details that will be executing the process.
load_ldap_detl = pickle.load(open("save.p", "rb"))
un = load_ldap_detl['User']
pw = load_ldap_detl['Password']

server='localhost'
database='MyStore'
cnxn=pyodbc.connect('DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={un};PWD={pw};Trusted_Connection=True;'
                    .format(server=server, database=database, un=un, pw=pw))
cursor = cnxn.cursor()

path = 'C:\\Simon\\DailySales\\Inbound\\'
Files = glob.glob(path + "Sales*.csv")

if Files.__len__() >= 1:
    logging.info('Processing files')
    for filename in Files:
        logging.info('Processing file {}'.format(filename))
        
        df = pd.read_csv(filename,engine='python', delimiter=',')
        df = df.fillna(value=-1)

        cursor = cnxn.cursor()
        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            for i in range(len(row)):
                if row[i] == '':
                    row[i] = None
            cursor.execute("INSERT INTO [stg].[DailySales] ([Product_Code],[Quantity],[Price],[SalesDate],[db_active_date],[srcFileName]) values(?,?,?,?,?,?)", row['Product_Code'],row['Quantity'],row['Price'],row['SalesDate'],datetime.datetime.now(),os.path.basename(filename))
        cnxn.commit()
        cursor.close()
        
        logging.info('File {} has been inserted into the database'.format(filename))
    
        try:
            shutil.move(filename, 'C:\\Simon\\DailySales\\Archive\\{}_{}'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S'),os.path.basename(filename)))
        except BaseException as e:
            print("Failed to archvie") 
            logging.error('File {} could not be archived {}.'.format(filename,datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
else:
    logging.error("Insufficient files to process!")
    
logging.info('Process has completed')
