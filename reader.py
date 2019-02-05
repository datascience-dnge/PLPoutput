#!/usr/bin/env python2.7

from pyspark.sql import SparkSession
from sys import argv, exit
from sqlFunc import getData, save2Hive
import time
       

def main(spark, csvpath, db, tdb, namestd, HiveMode):

        ############## ETL process ######################################################################
	t0=time.time()
#	hdfs_path="hdfs:///user/"+csvpath
        hdfs_path=csvpath
        tdf=time.time()	
        dfcen, dflin, dfser, dfemb, dfbar, dfeta = getData(spark, hdfs_path, db, tdb, namestd)
        tdf2=time.time()-tdf
	print "saved Dataframe Time: "+str(tdf2)+"s"

        ############ HIVE DATAWAREHOUSE process #########################################################
        tparquet=time.time()
        if HiveMode == 'new':
           spark.sql("create database if not exists "+db)
        save2Hive(dfcen, dflin, dfser, dfemb, dfbar, dfeta, db, tdb, HiveMode)
        tparquet2=time.time()-tparquet
 	print "saved database Time: "+str(tparquet2)+"s"

        #delete *.csv files saves in hdfs
        str_hdfs="hdfs dfs -rm -r -f " + hdfs_path+"/*.csv"
#        system(str_hdfs)

	t1=time.time()
	print "Elapsed Time: "+str(t1-t0)+"s"

if __name__=='__main__':
     
    hive_path="hdfs:///user/hive/warehouse"    
    if len(argv) < 5:
       print "you need run a submit process in this way: spark2-submnit --master yarn "'"path"'" "'"db"'" "'"tdb"'" "'"namsStd"'" "'"HiveMode"'" "
       print 'path: where live files in hdfs system.'
       print 'db: name of database where you save data.'
       print 'tdb: kind of data which values can be plp, pcp or chb'     
       print 'nameStd: name of study are you saving.'
       print 'HiveMode: new or append'

    path=argv[1]
    db=argv[2]
    tdb=argv[3]
    name_std=argv[4]
    HiveMode=argv[5]
    spark=SparkSession.builder.appName('CreateHiveDB').master('yarn')\
                      .enableHiveSupport().config("hive.metastore.warehouse.dir", hive_path)\
                      .config("hive.exec.dynamic.partition", "true")\
                      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    main(spark, path, db, tdb, name_std, HiveMode)
    spark.stop()	
