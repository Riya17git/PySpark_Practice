# Databricks notebook source
# MAGIC %md
# MAGIC ### Json Reading

# COMMAND ----------

df_json = spark.read.format('json').option('header',True)\
    .option('multiline',False)\
        .load('/Volumes/workspace/default/bigmart_volume/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.read.format('csv').option("header", "true").csv("/Volumes/workspace/default/bigmart_volume/BigMart Sales.csv")







# COMMAND ----------

/Volumes/workspace/default/bigmart_volume

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

                ''' 

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT 

# COMMAND ----------

from pyspark.sql.functions import col
#df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Condition

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Fat_Content')=='Regular')).display()

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Renamed

# COMMAND ----------


df.withColumnRenamed('Item_Weight','Item_Wt').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

from pyspark.sql.functions import lit
df = df.withColumn('flag',lit("new")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION and UNION BY NAME
# MAGIC

# COMMAND ----------

data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)


# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union

# COMMAND ----------

df1.union(df2).display()


# COMMAND ----------

data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union By Name

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Functions

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

