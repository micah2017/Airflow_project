import os,paramiko,sys,getopt
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
import pandas as pd
from jproperties import Properties
from sqlalchemy import create_engine
from pyspark.sql.types import StructType, StructField,DoubleType, IntegerType, StringType

SPARK_DIRECTORY = "~/installations/spark341/jars"
PATH_TO_JAR_FILE = SPARK_DIRECTORY+"/Postgres-connector.jar"
EXTRA_JAR_FILE = [SPARK_DIRECTORY+"/ojdbc8.jar"]

configs = Properties()

with open('../configurations/config.properties', 'rb') as config_file:
    configs.load(config_file)

# Local Staging file locations
v_data_dir = str(configs.get("data_directory").data).strip()

def get_spark_session(v_session_name):

    spark = SparkSession.builder.appName(v_session_name).config("spark.jars", PATH_TO_JAR_FILE)\
            .config("spark.driver.memory", "100g")\
            .config('spark.driver.extraClassPath', EXTRA_JAR_FILE)\
            .getOrCreate()

    return spark

def get_postgres_engine(v_con):
    v_con_string = 'postgresql+psycopg2://'+v_con.get('v_user')+':'+v_con.get('v_pswd')+'@'+v_con.get('v_ip')+'/'+v_con.get('v_db')
    engine = create_engine(v_con_string)
    return engine

def clean_df_headers(df):
    for col in df.columns:
                new_string = col.strip().replace('"', "").replace(" ", "_").replace("-", "_").lower()
                df = df.withColumnRenamed(col, new_string)
    return df
    
def extract_file_data(spark,file_format,path):
    if file_format=='json':
        df = spark.read.json(path)
    else:
        df = spark.read.option("inferSchema","true").option("header","true").format(file_format).load(path)
    
    df = clean_df_headers(df)
    return df

def extract_database_data(spark,v_query,v_con_object):

    df = spark.read \
        .format("jdbc") \
        .option("url", v_con_object.get('v_url')) \
        .option("query", v_query) \
        .option("user", v_con_object.get('v_user')) \
        .option("password", v_con_object.get('v_pswd')) \
        .option("numPartitions", "100") \
        .option("fetchsize","100000") \
        .option("driver", v_con_object.get('v_driver')) \
        .load()

    df = clean_df_headers(df)
    
    return df
    
def load_dataframe_data(df,v_target_table,v_write_mode,v_con_object): 
        
    df.repartition(25) \
        .write.mode(v_write_mode) \
        .format("jdbc") \
        .option("url",v_con_object.get('v_url')) \
        .option("user",v_con_object.get('v_user')) \
        .option("password", v_con_object.get('v_pswd')) \
        .option("driver", v_con_object.get('v_driver')) \
        .option("dbtable", v_target_table) \
        .option("fetchsize","100000") \
        .option('batchsize', '100000') \
        .option("numPartitions","500") \
        .option("isolationLevel", "NONE") \
        .save()

def get_sftp_file(conn_object):
    conn=conn_object.get('connect_object')  
    file_path=str(conn_object.get('file_path')).strip()
    
    SSH_Client= paramiko.SSHClient()
    SSH_Client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    SSH_Client.connect(hostname=conn.get('host'), 
                       # port=conn.get('port'), 
                       username=conn.get('username'),
                       password= conn.get('password'), 
                       look_for_keys=False)
    head, tail = os.path.split(file_path)
    localFilePath  = v_data_dir+"/"+tail
    remoteFilePath = file_path

    sftp_client    = SSH_Client.open_sftp()
    
    try:
        sftp_client.get(remoteFilePath, localFilePath)
        print(localFilePath,' downloaded successfully')
    except FileNotFoundError as err:
        print(f"File: {remoteFilePath} was not found on the source server",err)
        
    sftp_client.close()

#Configure your data sources in this function
def configure_data_sources(v_extraction_date):
    #NB if extraction date is part of sorce file then ensure its concatenated with file name
    # i.e f'/sqreamdata/bi_files/Mshwari/Test/{v_extraction_date}_test_nested_obj.json' 
    #For a file with date as prefix e.g 20250323_test_nested_obj.json
    file_config = str(configs.get("sftp_file_config").data).strip()
    return file_config
    
def ingest_data(v_extraction_date):
    files= configure_data_sources(v_extraction_date)
    #Initialize spark session
    spark = get_spark_session("FILE_EXTRACTION"+v_extraction_date)
    
    #Data warehouse connection object
    dw_connection = {
                'v_url':str(configs.get("DW_URL").data),
                'v_driver':str(configs.get("DW_DRIVER").data),
                'v_user':str(configs.get("DW_USER").data),
                'v_pswd':str(configs.get("DW_PASSWORD").data),
                'v_ip':str(configs.get("DW_IP_ADDRESS").data),
                'v_port':str(configs.get("DW_PORT").data),
                'v_db':str(configs.get("DW_DATABASE").data)
            }
    
    #Ingest all configured files to datawarehouse
    for file in files:
        if file.get('source_type')=='sftp_file':          
            head, tail = os.path.split(file.get('file_path'))
            file_format=str(tail).split('.')[1]
            local_file_path=v_data_dir+"/"+tail
            #copy sftp file to local staging environment
            get_sftp_file(file)
            #Extract the file to spark dataframe
            df = extract_file_data(spark,file_format,str(local_file_path).strip())
        elif file.get('source_type')=='database_file':
            v_query= str(file.get('query'))
            v_con_object = file.get('connect_object')
            df = extract_database_data(spark,v_query,v_con_object)

        df= df.withColumn('extraction_date', lit(v_extraction_date))
        df.printSchema()
       
    #Load the final transformed data to Datawarehouse
    load_dataframe_data(df,str(file.get('target_table')).strip(),'append',dw_connection)
    print('Loaded Succesfully')

def main(argv):
    
    v_date = ''
    
    try:
        opts, args = getopt.getopt(argv,"d:h:",["date=","help="])
    except getopt.GetoptError:
        print ('filename.py -d <date> -h <help>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print ('Syntax: filename.py -d <date> or  -h <help>')
            sys.exit()
        elif opt in ("-d", "--date"):
            v_date = arg
        else:
            print ('Invalid argument passed')
            sys.exit()

    ingest_data(v_date)
    

if __name__ == "__main__":
   main(sys.argv[1:])
