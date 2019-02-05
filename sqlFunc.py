from pyspark.sql.functions import lit, trim, col, size, current_date, unix_timestamp, to_date, lower, year, split
from pyspark.sql.types import DateType, StructType
from os import listdir, system, getenv
from sys import argv, exit
from datetime import datetime
#from createSemL import creSMplp

def sqlbar(sc,scen,columns):
    hc=HiveContext(sc)
    orgCol=('Simul','Eta','Blo','BarNum','BarNom','CMgBar','DemBar','PerBar')
    if set(columns) & set(orgCol) == set(columns):
       str_col=','.join(map(str,columns))
       str_tmp=' Simul='
       nscen=len(scen)
       i=0
       str_scen=''
       for s in scen:
           i+=1
           if i<nscen:
  	      str_scen+=str_tmp+str(s)+' or'
           else:
              str_scen+=str_tmp+str(s)
       str_sql="select "+str_col+' from pcpplp.pcpbar where'+str_scen
       df=hc.sql(str_sql)
    else:
       print 'Some features in tuple "columns" are not header in pcpbar table'   
       print str(orgCol)
       df=-1
    return df

def sqlcen(sc,scen,columns):
    hc=HiveContext(sc)
    orgCol=('Simul','Eta','Blo','CenNum','CenNom','CenTip','CenBar','BarNom','CenQgen','CenPgen','Tecnologia','PotMax')
    if set(columns) & set(orgCol) == set(columns):
       str_col=','.join(map(str,columns))
       str_tmp=' Simul='
       nscen=len(scen)
       i=0
       str_scen=''
       for s in scen:
           i+=1
           if i<nscen:
  	      str_scen+=str_tmp+str(s)+' or'
           else:
              str_scen+=str_tmp+str(s)
       str_sql="select "+str_col+' from pcpplp.pcpcen where'+str_scen
       df=hc.sql(str_sql)
    else:
       print 'Some features in tuple "columns" are not header in pcpcen table'   
       print str(orgCol)
       df=-1
    return df

def sqllin(sc,scen,columns):
    hc=HiveContext(sc)
    orgCol=('Simul','Eta','Blo','LinNum','LinNom','BarA','BarB','LinFlu','LinPer','LinPer2','LinIT_UM','FMax')
    if set(columns) & set(orgCol) == set(columns):
       str_col=','.join(map(str,columns))
       str_tmp=' Simul='
       nscen=len(scen)
       i=0
       str_scen=''
       for s in scen:
           i+=1
           if i<nscen:
  	      str_scen+=str_tmp+str(s)+' or'
           else:
              str_scen+=str_tmp+str(s)
       str_sql="select "+str_col+' from pcpplp.pcplin where'+str_scen
       df=hc.sql(str_sql)
    else:
       print 'Some features in tuple "columns" are not header in pcplin table'   
       print str(orgCol) 
       df=-1
    return df

def sqlser(sc,scen,columns):
    hc=HiveContext(sc)
    orgCol=('Simul','Eta','Blo','SerNum','SerNom','SerTip','SerBar','SerQGen','SerQVer','SerPSom','SerPGen')
    if set(columns) & set(orgCol) == set(columns):
       str_col=','.join(map(str,columns))
       str_tmp=' Simul='
       nscen=len(scen)
       i=0
       str_scen=''
       for s in scen:
           i+=1
           if i<nscen:
  	      str_scen+=str_tmp+str(s)+' or'
           else:
              str_scen+=str_tmp+str(s)
       str_sql="select "+str_col+' from pcpplp.pcpser where'+str_scen
       df=hc.sql(str_sql)
    else:
       print 'Some features in tuple "columns" are not header in pcpser table'   
       print str(orgCol)
       df=-1
    return df

def sqlemb(sc,scen,columns):
    hc=HiveContext(sc)
    orgCol=('Simul','Eta','Blo','EmbNum','EmbNom','EmbFac','EmbVini','EmbVfin','EmbQgen','EmbQver','EmbQdef','EmbPsom')
    if set(columns) & set(orgCol) == set(columns):
       str_col=','.join(map(str,columns))
       str_tmp=' Simul='
       nscen=len(scen)
       i=0
       str_scen=''
       for s in scen:
           i+=1
           if i<nscen:
  	      str_scen+=str_tmp+str(s)+' or'
           else:
              str_scen+=str_tmp+str(s)
       str_sql="select "+str_col+' from pcpplp.pcpemb where'+str_scen
       df=hc.sql(str_sql)
    else:
       print 'Some features in tuple "columns" are not header in pcpemb table'
       print str(orgCol)
       df=-1
    return df

def getSL(dfb, dfc, dfe, dford):
    
    #plpbar Semantic Layer
    dft1=dfb.join(dfe.select("BloqueE","TipoBlo","Duracion","Fecha","Categoria"), dfb.Bloque==dfe.BloqueE,"inner").drop(dfe.BloqueE)
    dft2=dft1.select("Fecha","Hidro","Categoria","Duracion", "BarNom", "CMgBar","DemBarE","User","DateStudy","NameStudy")
    dft2=dft2.withColumn("cruce",col("CMgBar")*col("Duracion")).withColumn("BarNom",trim(col("BarNom")))
    dft2=dft2.withColumn("Anio",year(dft2.Fecha)) 
    dft3=dft2.groupBy("Fecha","Categoria","Duracion","Hidro","BarNom","User","DateStudy","NameStudy","Anio")\
              .agg({'Duracion':'sum','cruce':'sum','DemBarE':'sum'}).withColumn("CMg",col("sum(cruce)")/col("sum(Duracion)"))    
    dft4=dft3.select("Fecha","Categoria","Duracion","Hidro","BarNom","CMg","sum(DemBarE)","User","DateStudy","NameStudy","Anio")
    dfbar2=dft4.withColumnRenamed("sum(DemBarE)","Demanda")#.withColumn("Fecha",to_date(col("Fecha")).cast("date"))
    
    #plpcensl
    dfcen2=dfc.join(dfe.select("BloqueE","TipoBlo","Duracion","Fecha","Categoria"), dfc.Bloque==dfe.BloqueE,"inner")\
            .drop(dfe.BloqueE)
    dfcen2=dfcen2.withColumn("Anio",year(dfcen2.Fecha))

    dfcen2=dfcen2.groupBy("Fecha","Hidro","Categoria","Duracion", "Type", "CenNom","BarNom","CenEgen","User","DateStudy","NameStudy")\
            .agg({'Duracion':'sum','CenEgen':'sum'})
    dfcen2=dfcen2.withColumn("Despacho",col("sum(CenEgen)")/col("sum(Duracion)"))\
             .withColumnRenamed("sum(Duracion)","Horas").withColumnRenamed("sum(CenEgen)","Produccion")
    dfcen2=dfcen2.selectExpr("Fecha","Hidro","Categoria","Type","Horas","CenNom","BarNom","Despacho","Produccion","User"\
                              ,"DateStudy","NameStudy")
    
    #mix plpbar y plpcen with ordhid

    dfcen3=dfcen2.join(dfbar2.select("Fecha","Hidro","Categoria","BarNom","CMg"),
                   (dfcen2.BarNom==dfbar2.BarNom) &\
                   (dfcen2.Fecha==dfbar2.Fecha) &\
                   (dfcen2.Categoria==dfbar2.Categoria) &\
                   (dfcen2.Hidro==dfbar2.Hidro),"inner").drop(dfbar2.Fecha).drop(dfbar2.Hidro)\
                   .drop(dfbar2.Categoria).drop(dfbar2.BarNom)  
    dfcen3=dfcen3.select("Fecha","Hidro","Categoria","CenNom","BarNom","CMg","Produccion")
    dfcen3=dfcen3.withColumn("Simul1",split(col("Hidro"),' ')[1].cast("int"))
    dfcen3=dfcen3.join(dford, (dfcen3.Fecha==dford.Fecha) & (dfcen3.Simul1==dford.Sim),"left")\
                 .drop(dford.Fecha).drop(dford.Sim).drop(dfcen3.Simul1).withColumnRenamed("Index","ordhid")
    
    return dfbar2, dfcen2, dfcen3
    
def getData(spark, hdfs_path, db, tdb, name_std):

 
        #put csv files into dataframe
        dfpcpcen=spark.read.format("csv").options(header="true",ignoreLeadingWhiteSpace='true',\
                            inferSchema='true').load(hdfs_path+"/"+tdb+"cen.csv")
        dfpcplin=spark.read.format("csv").options(header="true",ignoreLeadingWhiteSpace='true',\
                            inferSchema='true').load(hdfs_path+"/"+tdb+"lin.csv")
        dfpcpbar=spark.read.format("csv").options(header="true",ignoreLeadingWhiteSpace='true',\
                            inferSchema='true').load(hdfs_path+"/"+tdb+"bar.csv")
        dfpcpser=spark.read.format("csv").options(header="true",ignoreLeadingWhiteSpace='true',\
                            inferSchema='true').load(hdfs_path+"/"+tdb+"ser.csv")
        dfpcpemb=spark.read.format("csv").options(header="true",ignoreLeadingWhiteSpace='true',\
                            inferSchema='true').load(hdfs_path+"/"+tdb+"emb.csv")     

        user=getenv('USER')
        now_date=current_date()        

	#delete space in name's laber
        if tdb=='pcp':
           formato="yyyy-MM-dd HH:mm:ss"      
	   dfbar=dfpcpbar.withColumn("Simul",trim(col("Simul"))).withColumn("Simul",col("Simul").cast("int"))\
                         .withColumn("Date",trim(col("Date")))\
                         .withColumn('Date', (unix_timestamp('Date',formato)-4*3600).cast('timestamp'))\
                         .withColumn("Eta",trim(col("Eta"))).withColumn("Eta",col("Eta").cast("int"))\
                         .withColumn("Blo",trim(col("Blo"))).withColumn("Blo",col("Blo").cast("int"))\
                         .withColumn("BarNum",trim(col("BarNum"))).withColumn("BarNum",col("BarNum").cast("int"))\
                         .withColumn("CMgBar",trim(col("CMgBar"))).withColumn("CMgBar",col("CMgBar").cast("double"))\
                         .withColumn("DemBar",trim(col("DemBar"))).withColumn("DemBar",col("DemBar").cast("double"))\
                         .withColumn("PerBar",trim(col("PerBar"))).withColumn("PerBar",col("PerBar").cast("double"))\
                         .withColumn("BarNom",trim(col("BarNom")))

           dfcen=dfpcpcen.withColumn("Simul",trim(col("Simul"))).withColumn("Simul",col("Simul").cast("int"))\
                         .withColumn("Date",trim(col("Date")))\
                         .withColumn('Date', (unix_timestamp('Date',formato)-4*3600).cast('timestamp'))\
                         .withColumn("Eta",trim(col("Eta"))).withColumn("Eta",col("Eta").cast("int"))\
                         .withColumn("Blo",trim(col("Blo"))).withColumn("Blo",col("Blo").cast("int"))\
                         .withColumn("CenNum",trim(col("CenNum"))).withColumn("CenNum",col("CenNum").cast("int"))\
                         .withColumn("CenTip",trim(col("CenTip")))\
                         .withColumn("BarNom",trim(col("BarNom")))\
                         .withColumn("CenNom",trim(col("CenNom")))\
                         .withColumn("Tecnologia",trim(col("tecnologia")))\
                         .withColumn("CenBar",trim(col("CenBar"))).withColumn("CenBar",col("CenBar").cast("int"))\
                         .withColumn("CenQgen",trim(col("CenQgen"))).withColumn("CenQgen",col("CenQgen").cast("double"))\
                         .withColumn("CenPgen",trim(col("CenPgen"))).withColumn("CenPGen",col("CenPgen").cast("double"))\
                         .withColumn("PotMax",trim(col("PotMax"))).withColumn("PotMax",col("PotMax").cast("double"))

           dflin=dfpcplin.withColumn("Simul",trim(col("Simul"))).withColumn("Simul",col("Simul").cast("int"))\
                         .withColumn("Date",trim(col("Date")))\
                         .withColumn('Date', (unix_timestamp('Date',formato)-4*3600).cast('timestamp'))\
                         .withColumn("Eta",trim(col("Eta"))).withColumn("Eta",col("Eta").cast("int"))\
                         .withColumn("Blo",trim(col("Blo"))).withColumn("Blo",col("Blo").cast("int"))\
                         .withColumn("LinNum",trim(col("LinNum"))).withColumn("LinNum",col("LinNum").cast("int"))\
                         .withColumn("BarA",trim(col("BarA"))).withColumn("BarA",col("BarA").cast("int"))\
                         .withColumn("BarB",trim(col("BarB"))).withColumn("BarB",col("BarB").cast("int"))\
                         .withColumn("LinFlu",trim(col("LinFlu"))).withColumn("LinFlu",col("Linflu").cast("double"))\
                         .withColumn("LinPer",trim(col("LinPer"))).withColumn("LinPer",col("LinPer").cast("double"))\
                         .withColumn("LinPer2",trim(col("LinPer2"))).withColumn("LinPer2",col("LinPer2").cast("double"))\
                         .withColumn("LinIT_UM",trim(col("LinIT_UM"))).withColumn("LinIT_UM",col("LinIT_UM").cast("double"))\
                         .withColumn("FMax",trim(col("FMax"))).withColumn("FMax",col("FMax").cast("double"))\
                         .withColumn("LinNom",trim(col("LinNom")))

	   dfser=dfpcpser.withColumn("Simul",trim(col("Simul"))).withColumn("Simul",col("Simul").cast("int"))\
                         .withColumn("Date",trim(col("Date")))\
                         .withColumn('Date', (unix_timestamp('Date',formato)-4*3600).cast('timestamp'))\
                         .withColumn("Eta",trim(col("Eta"))).withColumn("Eta",col("Eta").cast("int"))\
                         .withColumn("Blo",trim(col("Blo"))).withColumn("Blo",col("Blo").cast("int"))\
                         .withColumn("SerNum",trim(col("SerNum"))).withColumn("SerNum",col("SerNum").cast("int"))\
                         .withColumn("SerTip",trim(col("SerTip")))\
                         .withColumn("SerBar",trim(col("SerBar"))).withColumn("SerBar",col("SerBar").cast("int"))\
                         .withColumn("SerQGen",trim(col("SerQGen"))).withColumn("SerQGen",col("SerQGen").cast("double"))\
                         .withColumn("SerQVer",trim(col("SerQVer"))).withColumn("SerQVer",col("SerQVer").cast("double"))\
                         .withColumn("SerPSom",trim(col("SerPSom"))).withColumn("SerPSom",col("SerPSom").cast("double"))\
                         .withColumn("SerPGen",trim(col("SerPGen"))).withColumn("SerPGen",col("SerPGen").cast("double"))\
                         .withColumn("SerNom",trim(col("SerNom")))
           
	   dfemb=dfpcpemb.withColumn("Simul",trim(col("Simul"))).withColumn("Simul",col("Simul").cast("int"))\
                         .withColumn("Date",trim(col("Date")))\
                         .withColumn('Date', (unix_timestamp('Date',formato)-4*3600).cast('timestamp'))\
                         .withColumn("Eta",trim(col("Eta"))).withColumn("Eta",col("Eta").cast("int"))\
                         .withColumn("Blo",trim(col("Blo"))).withColumn("Blo",col("Blo").cast("int"))\
                         .withColumn("EmbNum",trim(col("EmbNum"))).withColumn("EmbNum",col("EmbNum").cast("int"))\
                         .withColumn("EmbFac",trim(col("EmbFac"))).withColumn("EmbFac",col("EmbFac").cast("double"))\
                         .withColumn("EmbVini",trim(col("EmbVini"))).withColumn("EmbVini",col("EmbVini").cast("double"))\
                         .withColumn("EmbVfin",trim(col("EmbVfin"))).withColumn("EmbVfin",col("EmbVfin").cast("double"))\
                         .withColumn("EmbQgen",trim(col("EmbQgen"))).withColumn("EmbQgen",col("EmbQgen").cast("double"))\
                         .withColumn("EmbQver",trim(col("EmbQVer"))).withColumn("EmbQVer",col("EmbQVer").cast("double"))\
                         .withColumn("EmbQdef",trim(col("EmbQdef"))).withColumn("EmbQdef",col("EmbQdef").cast("double"))\
                         .withColumn("EmbPsom",trim(col("EmbPsom"))).withColumn("EmbPsom",col("EmbPsom").cast("double"))\
                         .withColumn("EmbNom",trim(col("EmbNom")))

           dfeta=[]

        elif tdb=='plp':
            formato='dd/MM/yyyy'
            dfpcpeta=spark.read.options(header="true",ignoreLeadingWhiteSpace='true',\
                         inferSchema='true',timestampFormat=formato).csv(hdfs_path+"/"+"etapas.csv")
            dfeta=dfpcpeta.withColumnRenamed("#Bloque","BloqueE")\
                         .withColumn("BloqueE", trim(col("BloqueE")))\
                         .withColumn("BloqueE",col('BloqueE').cast("int"))\
                         .withColumnRenamed(" Nombre","Nombre")\
                         .withColumn("Nombre", trim(col("Nombre")))\
                         .withColumnRenamed(" TipoEta","TipoEta")\
                         .withColumnRenamed(" TipoBlo","TipoBlo")\
                         .withColumnRenamed(" Anno","Anno")\
                         .withColumnRenamed(" Mes","Mes")\
                         .withColumnRenamed(" Duracion","Duracion")\
                         .withColumnRenamed(" FactEner","FactEner")\
                         .withColumnRenamed(" PctjMes","PctjMes")\
                         .withColumnRenamed(" Fecha","Fecha")\
                         .withColumnRenamed(" Categoria","Categoria")\
                         .withColumn("User",lit(user))\
                         .withColumn("DateStudy",lit(now_date))\
                         .withColumn('DateStudy',unix_timestamp('DateStudy',formato).cast('timestamp'))\
                         .withColumn("NameStudy", lit(name_std))

            dfbargeo=spark.read.format("csv").options(header="true",ignoreLeadingWhiteSpace='true',\
                                            inferSchema='true').load(hdfs_path+"/"+"barras.csv")
            dfgeo=dfbargeo.withColumnRenamed("#Numero","Numero")\
                         .withColumnRenamed(" Nombre","Nombre")\
                         .withColumn("Nombre", trim(col("Nombre")))\
                         .withColumn("Nombre",lower(col("Nombre")))\
                         .withColumnRenamed(" Longitud","Longitud")\
                         .withColumnRenamed(" Latitud", "Latitud")
                                     

            dfbar=dfpcpbar.withColumn('Bloque',col('Bloque').cast("int"))\
                         .withColumn('BarNum',col('BarNum').cast("int"))\
                         .withColumn('BarNom',trim(col('BarNom')))\
                         .withColumnRenamed("BarRetE ","BarRetE")\
                         .withColumn("User",lit(user))\
                         .withColumn("DateStudy",lit(now_date))\
                         .withColumn('DateStudy',unix_timestamp('DateStudy',formato).cast('timestamp'))\
                         .withColumn("NameStudy", lit(name_std))\
                         .withColumn("BarNom",lower(col("BarNom")))

            dfbar=dfbar.join(dfgeo,dfgeo["Nombre"]==dfbar.BarNom)            
            dfbar=dfbar.select("Hidro","Bloque","TipoEtapa","BarNom","BarNum","CMgBar","DemBarP",\
                              "DemBarE","PerBarP","PerBarE","BarRetP","BarRetE","Longitud","Latitud",\
                              "User",'DateStudy','NameStudy')

            dfcen=dfpcpcen.withColumnRenamed("CenCostOp ","CenCostOp")\
                         .withColumn('CenNom',trim(col('CenNom')))\
                         .withColumn('BarNom',trim(col('BarNom')))\
                         .withColumn('BarNom',lower(col('BarNom')))\
                         .withColumn("User",lit(user))\
                         .withColumn("DateStudy",lit(now_date))\
                         .withColumn('DateStudy',unix_timestamp('DateStudy',formato).cast('timestamp'))\
                         .withColumn("NameStudy", lit(name_std))
            dfcen=dfcen.select("Hidro","Bloque","TipoEtapa","CenNum","CenNom","CenTip","CenBar","BarNom","CenQgen","CenPgen",\
                              "CenEgen","CenInyP","CenInyE","CenRen","CenCVar","CenCostOp","User",'DateStudy','NameStudy')
            
            dfcenemp=spark.read.format("csv").options(header="true",ignoreLeadingWhiteSpace='true',\
                         inferSchema='true',timestampFormat=formato).csv(hdfs_path+"/"+"Empresas.csv")
            dfcen=dfcen.join(dfcenemp,dfcen.CenNom==dfcenemp.Central,"Left").drop(dfcenemp.Central).drop(dfcenemp.Tipo)
            dfcen=dfcen.withColumn("Type",split(col("CenNom"),'\.')[0])

            dflin=dfpcplin.withColumnRenamed("LinITE ","LinITe")\
                         .withColumn('LinNom',trim(col('LinNom')))\
                         .withColumn('LinITP',col('LinITP').cast('double'))\
                         .withColumn('LinITE',col('LinITE').cast('double'))\
                         .withColumn("User",lit(user))\
                         .withColumn("DateStudy",lit(now_date))\
                         .withColumn('DateStudy',unix_timestamp('DateStudy',formato).cast('timestamp'))\
                         .withColumn("NameStudy", lit(name_std))
            dflin=dflin.select("Hidro","Bloque","TipoEtapa","LinNum","LinNom","BarA","BarB","LinFluP",\
                              "LinFluE","LinPerP","LinPerE","LinPer2P","LinPer2E","LinITP","LinITE",\
                              "User",'DateStudy','NameStudy')

            dfser=dfpcpser.withColumnRenamed("SerAflu ","SerAflu")\
                         .withColumn('SerNom',trim(col('SerNom')))\
                         .withColumn('BarNom',trim(col('BarNom')))\
                         .withColumn("User",lit(user))\
                         .withColumn("DateStudy",lit(now_date))\
                         .withColumn('DateStudy',unix_timestamp('DateStudy',formato).cast('timestamp'))\
                         .withColumn("NameStudy", lit(name_std))
            dfser=dfser.select("Hidro","Bloque","TipoEtapa","SerNum","SerNom","SerBar","BarNom",\
                             "SerQGen","SerQVer","SerPSom","SerPSom2","SerPGen","SerAflu",\
                             "User",'DateStudy','NameStudy')

            dfemb=dfpcpemb.withColumnRenamed("EmbAflu ","EmbAflu")\
                         .withColumn('EmbNom',trim(col('EmbNom')))\
                         .withColumn('EmbFac',col('EmbFac').cast("double"))\
                         .withColumn("User",lit(user))\
                         .withColumn("DateStudy",lit(now_date))\
                         .withColumn('DateStudy',unix_timestamp('DateStudy',formato).cast('timestamp'))\
                         .withColumn("NameStudy", lit(name_std))
            dfemb=dfemb.select("Hidro","Bloque","TipoEtapa","EmbNum","EmbNom","EmbFac","EmbVini",\
                              "EmbVfin","EmbQgen","EmbQver","EmbQdef","EmbPSom","EmbPSom2",\
                              "EmbAflu","User",'DateStudy','NameStudy')
            dford=spark.read.format("csv").options(header="true",ignoreLeadingWhiteSpace='true',\
                 inferSchema='true',timestampFormat=formato).load(hdfs_path+"/ordhid.csv")
            dfbarsl, dfcensl, dfbarcen=getSL(dfbar, dfcen, dfeta, dford)

#           sembar=creSMplp(dfbar, dfcen, dfser, dfemb, dfeta, user, now_date, name_std)

        elif tdb=='chb':
             df=spark.read.format('jdbc').options(url='jdbc:sqlite//Study1_1_13.db',\
                           driver='org.sqlite.JDBC',dbtable='Bus').load()

        return dfcen, dflin, dfser, dfemb, dfbar, dfeta, dfbarsl, dfcensl, dfbarcen

def save2Hive(dfcen, dflin, dfser, dfemb, dfbar, dfeta, dfbarsl, dfcensl, dfbarcen, db, tdb, HiveMode):

        hivetdb='parquet'
        dbbar=db+"."+tdb+"bar"
        dbcen=db+"."+tdb+"cen"
        dblin=db+"."+tdb+"lin"
        dbser=db+"."+tdb+"ser"
        dbemb=db+"."+tdb+"emb"       
        dbeta=db+"."+tdb+"eta"
        
        dbbarsl=db+"."+tdb+"barsl"
        dbcensl=db+"."+tdb+"censl"
        dbbarcen=db+"."+tdb+"barcen"
        
        

        if tdb=='plp':
                if HiveMode=='new':
                    dfbar.write.format(hivetdb).partitionBy("Hidro").saveAsTable(dbbar)
                    dfcen.write.format(hivetdb).partitionBy("Hidro").saveAsTable(dbcen)
                    dflin.write.format(hivetdb).partitionBy("Hidro").saveAsTable(dblin)
                    dfser.write.format(hivetdb).partitionBy("Hidro").saveAsTable(dbser)
                    dfemb.write.format(hivetdb).partitionBy("Hidro").saveAsTable(dbemb)
                    dfbarsl.write.format(hivetdb).partitionBy("Hidro").saveAsTable(dbbarsl)
                    dfcensl.write.format(hivetdb).partitionBy("Hidro").saveAsTable(dbcensl)
                    dfbarcen.write.format(hivetdb).partitionBy("ordhid").saveAsTable(dbbarcen)
                    dfeta.write.format(hivetdb).saveAsTable(dbeta)               
                elif HiveMode=='append':
                    dfbar.write.format(hivetdb).mode(HiveMode).partitionBy("Hidro").saveAsTable(dbbar)
                    dfcen.write.format(hivetdb).mode(HiveMode).partitionBy("Hidro").saveAsTable(dbcen)
                    dflin.write.format(hivetdb).mode(HiveMode).partitionBy("Hidro").saveAsTable(dblin)
                    dfser.write.format(hivetdb).mode(HiveMode).partitionBy("Hidro").saveAsTable(dbser)
                    dfemb.write.format(hivetdb).mode(HiveMode).partitionBy("Hidro").saveAsTable(dbemb)
                    dfbarsl.write.format(hivetdb).mode(HiveMode).partitionBy("Hidro").saveAsTable(dbbarsl)
                    dfcensl.write.format(hivetdb).mode(HiveMode).partitionBy("Hidro").saveAsTable(dbcensl)
                    dfbarcen.write.format(hivetdb).mode(HiveMode).partitionBy("ordhid").saveAsTable(dbbarcen)
                    dfeta.write.format(hivetdb).mode(HiveMode).saveAsTable(dbeta)

        elif tdb=='pcp':
   	        dfbar.write.format(hivetdb).partitionBy("Simul").saveAsTable(dbbar)
        	dfcen.write.format(hivetdb).partitionBy("Simul").saveAsTable(dbcen)
        	dflin.write.format(hivetdb).partitionBy("Simul").saveAsTable(dblin)
        	dfser.write.format(hivetdb).partitionBy("Simul").saveAsTable(dbser)
         	dfemb.write.format(hivetdb).partitionBy("Simul").saveAsTable(dbemb)

