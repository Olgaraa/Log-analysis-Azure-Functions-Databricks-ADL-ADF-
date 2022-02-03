#insert all the csv files from ADLS into one DF
conf=dbutils.secrets.get (scope='mysqlKV', key='conf')
spark.conf.set(conf,dbutils.secrets.get(scope="mysqlKV",key="adls"))
#list_of_files=dbutils.fs.ls("abfss://logs@adls_storage.dfs.core.windows.net/")
df = spark.read.csv('abfss://logs@adls_storage.dfs.core.windows.net/')
df.show(20, False)

#split the rows into columns and format them
from pyspark.sql.functions import regexp_extract, col, split
df = df.withColumn('host', split(df['_c0'], ' - - ').getItem(0))\
.withColumn('timestamp', F.date_format(F.to_timestamp(regexp_extract(col('_c0'), '\d+/[A-Z]+[a-z]+/\d+:\d+:\d+:\d+.-\d+', 0),"dd/MMM/yyyy:HH:mm:ss Z"), "yyyy-MM-dd HH:mm:ss Z"))\
.withColumn('all', split(df['_c0'], '"').getItem(1))\
.withColumn('method', regexp_extract(col('all'), '[^\s]*', 0))\
.withColumn('protocol', regexp_extract(col('all'), 'HTTP.*', 0))\
.withColumn('endpoint', regexp_extract(col('all'), '/[^\s]*', 0))\
.drop('_c0', '_c1')

df.show(20, False)
 
#check for malformed records(the ones that don't have any spaces in between) and assign them to a new DF
malformed_records=df.filter(col("all").rlike("^[^\s]+$")).withColumnRenamed("all", "malformed_records").drop("host", "timestamp", "method", "protocol", "endpoint")
malformed_records.count()
malformed_records.show()
 
#write the malformed records to ADLS
import datetime
currentdate = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%s")
conf=dbutils.secrets.get (scope='mysqlKV', key='conf')
spark.conf.set(conf,dbutils.secrets.get(scope="mysqlKV",key="adls"))
destination_path="abfss://malformed@adls_storage.dfs.core.windows.net"
malformed_records.write.format('csv').option('sep', ',').save(destination_path + '/Malformed_CSV ' + currentdate)
