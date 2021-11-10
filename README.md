# pythonETL
Simple but robust python ETL script to load files into a sql database. Perfect for when you receive your data as files (such as .csv) via some process and need to stage it to use it for warehousing or reporting.

## Set up logging to console
```python
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
```

## Set a format which is simpler for console use
```python
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)
logging.info('Process has started')
```

## Load/write
```python
ldap_detl = {"User": "Simon", "Password": "12345"}
pickle.dump( ldap_detl, open( "save.p", "wb" ))
```

## Load user details that will be executing the process.
```python
load_ldap_detl = pickle.load(open("save.p", "rb"))
un = load_ldap_detl['User']
pw = load_ldap_detl['Password']
server='localhost'
database='MyStore'
cnxn=pyodbc.connect('DRIVER=SQL Server;SERVER={server};DATABASE={database};UID={un};PWD={pw};Trusted_Connection=True;'.format(server=server, database=database, un=un, pw=pw))
cursor = cnxn.cursor()
```

## File details 
```python
path = 'C:\\Simon\\DailySales\\Inbound\\'
Files = glob.glob(path + "Sales*.csv")
```

## Read file and replace nulls/blanks with -1 (Design preference)   
```python
df = pd.read_csv(filename,engine='python', delimiter=',')
df = df.fillna(value=-1)
```

## Insert Dataframe into SQL Server:
```python
for index, row in df.iterrows():
    for i in range(len(row)):
        if row[i] == '':
            row[i] = None
    cursor.execute("INSERT INTO [stg].[DailySales] ([Product_Code],[Quantity],[Price],[SalesDate],[db_active_date],[srcFileName]) 
    values(?,?,?,?,?,?)", 
    row['Product_Code'],row['Quantity'],row['Price'],row['SalesDate'],datetime.datetime.now(),os.path.basename(filename))
cnxn.commit()
cursor.close()`
        
## Move file to archive location once file has been processed
`try:
    shutil.move(filename, 'C:\\Simon\\DailySales\\Archive\\{}_{}'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S'),os.path.basename(filename)))
except BaseException as e:
    print("Failed to archvie") 
    logging.error('File {} could not be archived {}.'.format(filename,datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
```
