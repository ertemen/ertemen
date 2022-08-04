import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import requests
import psycopg2 as pg
import pandas as pd
from datetime import datetime
import psycopg2.extras as extras

pd.set_option('display.max_columns', 20)

args = {'owner': 'airflow'}

default_args = {
    'owner': 'airflow',
    # 'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG("test", default_args=default_args, schedule_interval=timedelta(1))


# USD is the pivot currency
url = 'https://v6.exchangerate-api.com/v6/9227005e1c0cb3cc0a373690/latest/USD'

# get json data
response = requests.get(url)
data = response.json()

#get code and ratio from the dictionary
df=pd.DataFrame(list(data["conversion_rates"].items()),columns=['code','ratio'])
#get this to calc the date column - when reconcile we could use this
df['date'] = datetime.fromtimestamp(data["time_last_update_unix"]).date()
#calc extraction time
df['extract_time'] =datetime.today()
#ensure date is date in case we use it later on
df['date']=pd.to_datetime(df['date'])
#sort columns names
df = df[['code', 'ratio', 'date', 'extract_time']]


def populate_conv(conn, df, table):
    # tuples of rows, use it in values portion of SQL
    tuples = [tuple(x) for x in df.to_numpy()]
    #print(tuples)

    cols = ','.join(list(df.columns))
    # SQL query to execute if new code, date pair is introduced insert otherwise don't touch the data except the extraction time
    query = '''
            INSERT INTO %s(%s) VALUES %%s
            ON CONFLICT (code,date) DO UPDATE 
            SET (extract_time) = ROW(EXCLUDED.extract_time)
            ''' % (table, cols)
    cursor = conn.cursor()
    try:
        #fix - found on postgre docpsycopyg2
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    #print("the dataframe is inserted/updated")
    cursor.close()

def populate_month_conv(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))
    # SQL query to execute if new month insert as it is otherwise, mean is calculated so update ratio and upload time
    query = '''
            INSERT INTO %s(%s) VALUES %%s
            ON CONFLICT (code,month) DO UPDATE 
            SET (avg_ratio,last_upload) = ROW(EXCLUDED.avg_ratio,EXCLUDED.last_upload)
            ''' % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    #print("the dataframe is inserted")
    cursor.close()

conn = pg.connect(
        host="kandula.db.elephantsql.com",
        database="cgrgzozk",
        user="cgrgzozk",
        password="ev_TbSxcdG-l2Ka0uFYXiBvL9tgXyPHo")



# create the tables if it does not already exist
cursor = conn.cursor()
cursor.execute("""
     CREATE TABLE IF NOT EXISTS conversion_rates_ee (
        code varchar(50),
        ratio float,
        date date,
        extract_time timestamp,
        CONSTRAINT code_date UNIQUE (code, date)
    );

     CREATE TABLE IF NOT EXISTS monthly_conversion_rates_ee (
        month int,
        code varchar(50),
        avg_ratio float,
        last_upload timestamp,
        CONSTRAINT month_code UNIQUE (month, code)
    );

   
"""
               )
conn.commit()


# populate the incremental daily load table
populate_conv(conn, df, 'conversion_rates_ee')


# read conversation rates from db as the previous months are closed only interested in the current month
con_ratedf =pd.read_sql("select * from conversion_rates_ee where extract_time > date_trunc('month', CURRENT_DATE)",conn)

# calc yearmonth YYYYMM as integer
con_ratedf['month'] = con_ratedf["date"].map(lambda x:100*x.year + x.month)

# calculate the monthly average
monthly_avg_df = con_ratedf.groupby(['month','code'])['ratio'].mean().reset_index()
monthly_avg_df['last_upload'] =datetime.today().date()
monthly_avg_df.rename(columns={'ratio':'avg_ratio'},inplace=True)

#test_df = monthly_avg_df.loc[(monthly_avg_df.code=='CAD')]
#print(monthly_avg_df.head(40))
#populate the monthly conversion table
populate_month_conv(conn,monthly_avg_df,'monthly_conversion_rates_ee')






