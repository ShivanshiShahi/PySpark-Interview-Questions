## PySpark Interview Questions

#### SparkSession

SparkSession introduced in version Spark 2.0, It is an entry point to underlying Spark functionality in order to programmatically create Spark RDD, DataFrame and DataSet. SparkSession’s object spark is default available in spark-shell and it can be created programmatically using SparkSession builder pattern.

#### SparkContext

Since Spark 1.x, Spark SparkContext is an entry point to Spark.

#### Apache Spark

Apache Spark is an Open source analytical processing engine for large scale powerful distributed data processing and machine learning applications.

#### Apache Spark Features

Distributed processing using parallelize
Lazy evaluation

Applications running on Spark are 100x faster than traditional systems.

#### Apache Spark Architecture

Apache Spark works in a master-slave architecture where the master is called “Driver” and slaves are called “Workers”. When you run a Spark application, Spark Driver creates a context that is an entry point to your application, and all operations (transformations and actions) are executed on worker nodes, and the resources are managed by Cluster Manager.

![image](https://user-images.githubusercontent.com/97865583/196015131-b9af8bf3-59f1-4c65-adb5-5adbec4975f6.png)

#### create a PySpark DataFrame

You can manually create a PySpark DataFrame using toDF() and createDataFrame() methods.
```pyspark
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
```
#### Create DataFrame from RDD
```pyspark
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)
Using toDF() function

dfFromRDD1 = rdd.toDF()
```
#### Create DataFrame with schema
```pyspark
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
```
#### Pandas vs PySpark

Differences between the Pandas & PySpark, operations on Pyspark run faster than Pandas due to its distributed nature and parallel execution on multiple cores and machines. In other words, pandas run operations on a single node whereas PySpark runs on multiple machines. If you are working on a Machine Learning application where you are dealing with larger datasets, PySpark processes operations many times faster than pandas.

#### ArrayType vs MapType

ArrayType and MapType to define the DataFrame columns for array and map collections respectively. In the below example, column hobbies are defined as ArrayType(StringType) and properties defined as MapType(StringType,StringType) meaning both key and value as String.

#### collect() vs select()

Usually, collect() is used to retrieve the action output when you have very small result set and calling collect() on an RDD/DataFrame with a bigger resultset causes out of memory as it returns the entire dataset (from all workers) to the driver hence we should avoid calling collect() on a larger dataset.

select() is a transformation that returns a new DataFrame and holds the columns that are selected whereas collect() is an action that returns the entire data set in an Array to the driver.

#### PySpark Filter like and rlike
```pyspark
data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]

df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()

# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()
```
#### PySpark Join Types

PySpark Join is used to combine two DataFrames and by chaining these you can join multiple DataFrames; it supports all basic join type operations available in traditional SQL like INNER, LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF JOIN.
```pyspark
emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]

empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]

deptColumns = ["dept_name","dept_id"]

deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
```
#### PySpark Inner Join DataFrame

Inner join is the default join in PySpark and it’s mostly used. This joins two datasets on key columns, where keys that don't match the rows get dropped from both datasets (emp & dept).
```pyspark
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)
```
#### PySpark Full Outer Join

Outer a.k.a full, full outer join returns all rows from both datasets, where join expression doesn’t match it returns null on respective record columns.
```pyspark
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter") \
    .show(truncate=False)
```
#### PySpark Left Outer Join

Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for that record and drops records from right where match is not found.
```pyspark
empDF.join(deptDF,empDF("emp_dept_id") ==  deptDF("dept_id"),"left")
.show(false)
empDF.join(deptDF,empDF("emp_dept_id") ==  deptDF("dept_id"),"leftouter")
.show(false)
```
#### Right Outer Join

Right a.k.a Right Outer join is opposite of left join, here it returns all rows from the right dataset regardless of match found on the left dataset, when join expression doesn’t match, it assigns null for that record and drops records from left where match is not found.
```pyspark
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
   .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter") \
   .show(truncate=False)
```
#### Left Semi Join

left semi join is similar to inner join difference being left semi join returns all columns from the left dataset and ignores all columns from the right dataset. In other words, this join returns columns from the only left dataset for the records matched in the right dataset on join expression, records not matched on join expression are ignored from both left and right datasets. The same result can be achieved using select on the result of the inner join however, using this join would be efficient.
```pyspark
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi") \
   .show(truncate=False)
```
#### Left Anti Join

left anti join does the exact opposite of the left semi, left anti join returns only columns from the left dataset for non-matched records.
```pyspark
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti") \
   .show(truncate=False)
```
#### PySpark Self Join

Joins are not complete without a self join, Though there is no self-join type available, we can use any of the above-explained join types to join DataFrame to itself. Below is an example using inner self join.
```pyspark
empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)
```
#### PySpark Union and UnionAll

PySpark union() and unionAll() transformations are used to merge two or more DataFrame’s of the same schema or structure.
```pyspark
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

unionDF = df.union(df2)

unionAllDF = df.unionAll(df2)
```
Returns the same output as above.

#### Merge without Duplicates
```pyspark
disDF = df.union(df2).distinct()
```
#### PySpark UDF (User Defined Function)

PySpark UDF (a.k.a User Defined Function) is the most useful feature of Spark SQL & DataFrame that is used to extend the PySpark build in capabilities. UDF’s are the most expensive operations hence use them only when you have no choice and when essential.
```pyspark
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

Create a Python Function

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 
```
#### Convert a Python function to PySpark UDF
```pyspark
""" Converting function to UDF """
convertUDF = udf(lambda z: convertCase(z),StringType())

""" Converting function to UDF 
StringType() is by default hence not required """
convertUDF = udf(lambda z: convertCase(z)) 
```
#### Using UDF with PySpark DataFrame select()
```pyspark
df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)
```
#### PySpark map()

PySpark map (map()) is an RDD transformation that is used to apply the transformation function (lambda) on every element of RDD/DataFrame and returns a new RDD.
```pyspark
data = ["Project","Gutenberg’s","Alice’s","Adventures",
"in","Wonderland","Project","Gutenberg’s","Adventures",
"in","Wonderland","Project","Gutenberg’s"]

rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)
```
#### PySpark flatMap()

PySpark flatMap() is a transformation operation that flattens the RDD/DataFrame (array/map DataFrame columns) after applying the function on every element and returns a new PySpark RDD/DataFrame.
```pyspark
data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.pa
rallelize(data)
for element in rdd.collect():
    print(element)

rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)
```
#### PySpark fillna() & fill() – Replace NULL/None Values
```pyspark
filePath="resources/small_zipcode.csv"
df = spark.read.options(header='true', inferSchema='true') \
          .csv(filePath)

#Replace 0 for null for all integer columns
df.na.fill(value=0).show()

PySpark Replace Null/None Value with Empty String

df.na.fill("").show(false)
```
#### Replace NULL’s on specific columns

Now, let’s replace NULL’s on specific columns, below example replace column type with empty string and column city with value “unknown”.
```pyspark
df.na.fill("unknown",["city"]) \
    .na.fill("",["type"]).show()
```
Alternatively you can also write the above statement as
```pyspark
df.na.fill({"city": "unknown", "type": ""}) \
    .show()
```
#### PySpark Pivot and Unpivot DataFrame

PySpark pivot() function is used to rotate/transpose the data from one column into multiple Dataframe columns and back using unpivot(). Pivot() It is an aggregation where one of the grouping columns values is transposed into individual columns with distinct data.
```pyspark
data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
```
#### Pivot PySpark DataFrame

PySpark SQL provides pivot() function to rotate the data from one column into multiple columns. It is an aggregation where one of the grouping column values is transposed into individual columns with distinct data. To get the total amount exported to each country of each product, I will group by Product, pivot by Country, and the sum of Amount.
```pyspark
pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
```
This will transpose the countries from DataFrame rows into columns and produce the below output. where ever data is not present, it represents null by default.

#### Unpivot PySpark DataFrame

Unpivot is a reverse operation, we can achieve by rotating column values into rows values. PySpark SQL doesn’t have an unpivot function hence will use the stack() function.
```pyspark
from pyspark.sql.functions import expr
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
unPivotDF = pivotDF.select("Product", expr(unpivotExpr)) \
    .where("Total is not null")
```
It converts the pivoted column “country” to rows.

#### PySpark partitionBy() – Write to Disk

PySpark partitionBy() is a function of pyspark.sql.DataFrameWriter class which is used to partition the large dataset (DataFrame) into smaller files based on one or multiple columns while writing to disk. As you are aware PySpark is designed to process large datasets 100x faster than the traditional processing, this wouldn’t have been possible without partition. Below are some of the advantages of using PySpark partitions on memory or on disk.

Fast accessed to the data: Provides the ability to perform an operation on a smaller dataset.
```pyspark
df=spark.read.option("header",True) \
        .csv("/tmp/resources/simple-zipcodes.csv")
df.printSchema()

#partitionBy()
df.write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state")

#partitionBy() multiple columns
df.write.option("header",True) \
        .partitionBy("state","city") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state")
```
#### Data Skew – Control Number of Records per Partition File

Use option maxRecordsPerFile if you want to control the number of records for each partition. This is particularly helpful when your data is skewed (Having some partitions with very low records and other partitions with high numbers of records).
```pyspark
#partitionBy() control number of partitions
df.write.option("header",True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state")
```
#### Read a Specific Partition

Reads are much faster on partitioned data. This code snippet retrieves the data from a specific partition "state=AL and city=SPRINGVILLE". Here, It just reads the data from that specific folder instead of scanning a whole file (when not partitioned).
```pyspark
dfSinglePart=spark.read.option("header",True) \
            .csv("c:/tmp/zipcodes-state/state=AL/city=SPRINGVILLE")
```
#### explode()

Use explode() function to create a new row for each element in the given array column.
```pyspark
from pyspark.sql.functions import explode
df.select(df.name,explode(df.languagesAtSchool)).show()
```
#### split()

split() sql function returns an array type after splitting the string column by delimiter. Below example split the name column by comma delimiter.
```pyspark
from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()
```
#### PySpark Aggregate Functions

PySpark provides built-in standard Aggregate functions defined in DataFrame API, these come in handy when we need to make aggregate operations on DataFrame columns. Aggregate functions operate on a group of rows and calculate a single return value for every group.
```pyspark
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
```
#### mean()

This function returns the average of the values in a column. Alias for Avg
```pyspark
df.select(mean("salary")).show(truncate=False)
```
#### sum()

This function Returns the sum of all values in a column.
```pyspark
df.select(sum("salary")).show(truncate=False)
```
#### PySpark Window Functions

PySpark Window functions are used to calculate results such as the rank, row number e.t.c over a range of input rows.
```pyspark
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
```
#### row_number()

This window function is used to give the sequential row number starting from 1 to the result of each window partition.
```pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)
```
#### rank()

This window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.
```pyspark
"""rank"""
from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(windowSpec)) \
    .show()
```
#### dense_rank()

This window function is used to get the result with rank of rows within a window partition without any gaps. This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.
```pyspark
"""dens_rank"""
from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()
```
#### ntile() 

This window function returns the relative rank of result rows within a window partition. In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)
```pyspark
"""ntile"""
from pyspark.sql.functions import ntile
df.withColumn("ntile",ntile(2).over(windowSpec)) \
    .show()
```
#### LAG vs LEAD

LAG function is an analytic function that lets you query more than one row in a table at a time without having to join the table to itself. It returns values from a previous row in the table. To return a value from the next row, try using the LEAD function.
```pyspark
"""lag"""
from pyspark.sql.functions import lag    
df.withColumn("lag",lag("salary",2).over(windowSpec)) \
      .show()

 """lead"""
from pyspark.sql.functions import lead    
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
    .show()
```
#### PySpark Window Aggregate Functions
```pyspark
windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()
```
#### PySpark SQL Date and Timestamp Functions

DateType default format is yyyy-MM-dd 

TimestampType default format is yyyy-MM-dd HH:mm:ss.SSSS

Returns null if the input is a string that can not be cast to Date or Timestamp.
```pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()
data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
```
#### current_date()

Use current_date() to get the current system date. By default, the data will be returned in yyyy-dd-mm format.
```pyspark
#current_date()
df.select(current_date().alias("current_date")
  ).show(1)

#datediff()
df.select(col("input"), 
    datediff(current_date(),col("input")).alias("datediff")  
  ).show()
```
#### months_between()

The below example returns the months between two dates using months_between().
```pyspark
#months_between()
df.select(col("input"), 
    months_between(current_date(),col("input")).alias("months_between")  
  ).show()
```
Here we are adding and subtracting date and month from a given input.

#### add_months() , date_add(), date_sub()
```pyspark
df.select(col("input"), 
    add_months(col("input"),3).alias("add_months"), 
    add_months(col("input"),-3).alias("sub_months"), 
    date_add(col("input"),4).alias("date_add"), 
    date_sub(col("input"),4).alias("date_sub") 
  ).show()
```
#### year(), month(), month(),next_day(), weekofyear()
```pyspark
df.select(col("input"), 
     year(col("input")).alias("year"), 
     month(col("input")).alias("month"), 
     next_day(col("input"),"Sunday").alias("next_day"), 
     weekofyear(col("input")).alias("weekofyear") 
  ).show()
```
#### dayofweek(), dayofmonth(), dayofyear()
```pyspark
df.select(col("input"),  
     dayofweek(col("input")).alias("dayofweek"), 
     dayofmonth(col("input")).alias("dayofmonth"), 
     dayofyear(col("input")).alias("dayofyear"), 
  ).show()
```
#### PySpark JSON Functions

PySpark JSON functions are used to query or extract the elements from JSON string of DataFrame column by path, convert it to struct, mapt type e.t.c.

#### Create DataFrame with Column contains JSON String
```pyspark
from pyspark.sql import SparkSession,Row
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df=spark.createDataFrame([(1, jsonString)],["id","value"])
```
#### from_json()

PySpark from_json() function is used to convert JSON string into Struct type or Map type.
```pyspark
#Convert JSON string column to Map type
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))
```
#### to_json()

to_json() function is used to convert DataFrame columns MapType or Struct type to JSON string.
```pyspark
from pyspark.sql.functions import to_json,col
df2.withColumn("value",to_json(col("value"))) \
   .show(truncate=False)
```
#### PySpark Read CSV file into DataFrame

Using Header Record For Column Names
```pyspark
df2 = spark.read.option("header",True) \
     .csv("/tmp/resources/zipcodes.csv")
```
#### Read Multiple CSV Files
```pyspark
df = spark.read.csv("path1,path2,path3")
```
#### Read all CSV Files in a Directory
```pyspark
df = spark.read.csv("Folder path")
```
#### Options While Reading CSV File
```pyspark
df3 = spark.read.options(delimiter=',') \
  .csv("C:/apps/sparkbyexamples/src/pyspark-examples/resources/zipcodes.csv")

df4 = spark.read.options(inferSchema='True',delimiter=',') \
  .csv("src/main/resources/zipcodes.csv")
```
#### PySpark Read and Write Parquet File

Apache Parquet file is a columnar storage format available to any project in the Hadoop ecosystem, regardless of the choice of data processing framework, data model, or programming language. While querying columnar storage, it skips the non relevant data very quickly, making faster query execution. As a result aggregation queries consume less time compared to row-oriented databases.
```pyspark
data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)

df.write.parquet("/tmp/output/people.parquet")
```
#### Append or Overwrite an existing Parquet file
```pyspark
df.write.mode('append').parquet("/tmp/output/people.parquet")
df.write.mode('overwrite').parquet("/tmp/output/people.parquet")
```
#### PySpark When Otherwise | SQL Case When
```pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

from pyspark.sql.functions import when
df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
                                 .when(df.gender == "F","Female")
                                 .when(df.gender.isNull() ,"")
                                 .otherwise(df.gender))
df2.show()

df2=df.select(col("*"),when(df.gender == "M","Male")
                  .when(df.gender == "F","Female")
                  .when(df.gender.isNull() ,"")
                  .otherwise(df.gender).alias("new_gender"))
```
#### PySpark SQL expr() (Expression ) Function
```pyspark
#Concatenate columns using || (sql like)
data=[("James","Bond"),("Scott","Varsa")] 
df=spark.createDataFrame(data).toDF("col1","col2") 
df.withColumn("Name",expr(" col1 ||','|| col2")).show()
```
#### Using Filter with expr()
```pyspark
#Use expr()  to filter the rows
from pyspark.sql.functions import expr
data=[(100,2),(200,3000),(500,500)] 
df=spark.createDataFrame(data).toDF("col1","col2") 
df.filter(expr("col1 == col2")).show()
```
#### PySpark to_timestamp() – Convert String to Timestamp type
```pyspark
from pyspark.sql.functions import *

df=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df.printSchema()

#Timestamp String to DateType
df.withColumn("timestamp",to_timestamp("input_timestamp")) \
  .show(truncate=False)
```
#### Using Cast to convert TimestampType to DateType
```pyspark
df.withColumn('timestamp_string', \
         to_timestamp('timestamp').cast('string')) \
  .show(truncate=False)
```
#### PySpark to_date() – Convert Timestamp to Date
```pyspark
df=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df.printSchema()
```
#### Using to_date() – Convert Timestamp String to Date
```pyspark
from pyspark.sql.functions import *

#Timestamp String to DateType
df.withColumn("date_type",to_date("input_timestamp")) \
  .show(truncate=False)
```
#### PySpark

PySpark is an Apache Spark interface in Python. It is used for collaborating with Spark using APIs written in Python. It also supports Spark’s features like Spark DataFrame, Spark SQL, Spark Streaming, Spark MLlib and Spark Core. It provides an interactive PySpark shell to analyze structured and semi-structured data in a distributed environment. PySpark supports reading data from multiple sources and different formats. It also facilitates the use of RDDs (Resilient Distributed Datasets).

PySpark SparkContext is an initial entry point of the spark functionality. It also represents Spark Cluster Connection and can be used for creating the Spark RDDs (Resilient Distributed Datasets).

#### Resilient Distributed Datasets

RDDs expand to Resilient Distributed Datasets. These are the elements that are used for running and operating on multiple nodes to perform parallel processing on a cluster. Since RDDs are suited for parallel processing, they are immutable elements. This means that once we create RDD, we cannot modify it. RDDs are also fault-tolerant which means that whenever failure happens, they can be recovered automatically.

#### Transformation vs Action

Transformation: These operations when applied on RDDs result in the creation of a new RDD. Some of the examples of transformation operations are filter, groupBy, map.

Action: These operations instruct Spark to perform some computations on the RDD and return the result to the driver. It sends data from the Executer to the driver. count(), collect(), take() are some of the examples.

#### MLlib

Similar to Spark, PySpark provides a machine learning API which is known as MLlib that supports various ML algorithms like:

mllib.classification − This supports different methods for binary or multiclass classification and regression analysis like Random Forest, Decision Tree, Naive Bayes etc.

mllib.clustering − This is used for solving clustering problems that aim in grouping entities subsets with one another depending on similarity.

#### Cluster manager

A cluster manager is a cluster mode platform that helps to run Spark by providing all resources to worker nodes based on the requirements.

#### PySpark supports the following cluster manager types:

Standalone – This is a simple cluster manager that is included with Spark.

Apache Mesos – This manager can run Hadoop MapReduce and PySpark apps.

Hadoop YARN – This manager is used in Hadoop2.

Kubernetes – This is an open-source cluster manager that helps in automated deployment, scaling and automatic management of containerized apps.

local – This is simply a mode for running Spark applications on laptops/desktops.

#### PySpark RDDs have the following advantages:

In-Memory Processing: PySpark’s RDD helps in loading data from the disk to the memory. The RDDs can even be persisted in the memory for reusing the computations.

Immutability: The RDDs are immutable which means that once created, they cannot be modified. While applying any transformation operations on the RDDs, a new RDD would be created.

Fault Tolerance: The RDDs are fault-tolerant. This means that whenever an operation fails, the data gets automatically reloaded from other available partitions. This results in seamless execution of the PySpark applications.

Lazy Evolution: The PySpark transformation operations are not performed as soon as they are encountered. The operations would be stored in the DAG and are evaluated once it finds the first RDD action.

Partitioning: Whenever RDD is created from any data, the elements in the RDD are partitioned to the cores available by default.

#### Why PySpark is faster than pandas?

PySpark supports parallel execution of statements in a distributed environment, i.e on different cores and different machines which are not present in Pandas. This is why PySpark is faster than pandas.

#### DataFrame

PySpark DataFrame is a distributed collection of well-organized data that is equivalent to tables of the relational databases and are placed into named columns. The data in the PySpark DataFrame is distributed across different machines in the cluster and the operations performed on this would be run parallelly on all the machines. These can handle a large collection of structured or semi-structured data of a range of petabytes.

#### Shared variables in PySpark

Whenever PySpark performs the transformation operation using filter(), map() or reduce(), they are run on a remote node that uses the variables shipped with tasks. These variables are not reusable and cannot be shared across different tasks because they are not returned to the Driver. To solve the issue of reusability and sharing, we have shared variables in PySpark. There are two types of shared variables, they are:

#### Broadcast vs Accumulator

Broadcast variables: These are also known as read-only shared variables and are used in cases of data lookup requirements. These variables are cached and are made available on all the cluster nodes so that the tasks can make use of them. The variables are not sent with every task. They are rather distributed to the nodes using efficient algorithms for reducing the cost of communication.

Accumulator variables: These variables are called updatable shared variables. They are added through associative and commutative operations and are used for performing counter or sum operations.

#### Direct Acyclic Graph

DAG stands for Direct Acyclic Graph. DAGScheduler constitutes the scheduling layer of Spark which implements scheduling of tasks in a stage-oriented manner using jobs and stages. The logical execution plan (Dependencies lineage of transformation actions upon RDDs) is transformed into a physical execution plan consisting of stages. It computes a DAG of stages needed for each job and keeps track of what stages are RDDs are materialized and finds a minimal schedule for running the jobs. These stages are then submitted to TaskScheduler for running the stages.

#### Common workflow followed by the spark program

The first step is to create input RDDs depending on the external data. Data can be obtained from different data sources.

Post RDD creation, the RDD transformation operations like filter() or map() are run for creating new RDDs depending on the business logic.

If any intermediate RDDs are required to be reused for later purposes, we can persist those RDDs.

Lastly, if any action operations like first(), count() etc are present then spark launches it to initiate parallel computation.

#### SparkConf

PySpark SparkConf is used for setting the configurations and parameters required to run applications on a cluster or local system.

#### Code for creating SparkSession:
```pyspark
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") 
                   .appName('InterviewBitSparkSession') 
                   .getOrCreate()
```
master() – This is used for setting up the mode in which the application has to run - cluster mode (use the master name) or standalone mode. For Standalone mode, we use the local[x] value to the function, where x represents partition count to be created in RDD, DataFrame and DataSet. The value of x is ideally the number of CPU cores available.

appName() - Used for setting the application name.

getOrCreate() – For returning SparkSession object. This creates a new object if it does not exist. If an object is there, it simply returns that.

#### createDataFrame()

We can do it by making use of the createDataFrame() method of the SparkSession.
```pyspark
data = [('Harry', 20),
       ('Ron', 20),
       ('Hermoine', 20)]

columns = ["Name","Age"]

df = spark.createDataFrame(data=data, schema = columns)
```
#### startsWith() vs endsWith()

startsWith() – returns boolean Boolean value. It is true when the value of the column starts with the specified string and False when the match is not satisfied in that column value.

endsWith() – returns boolean Boolean value. It is true when the value of the column ends with the specified string and False when the match is not satisfied in that column value.
```pyspark
df.filter(col("Name").startsWith("H")).show()
``
#### PySpark SQL

PySpark SQL is the most popular PySpark module that is used to process structured columnar data. Once a DataFrame is created, we can interact with data using the SQL syntax. Spark SQL is used for bringing native raw SQL queries on Spark by using select, where, group by, join, union etc. For using PySpark SQL, the first step is to create a temporary table on DataFrame by using createOrReplaceTempView() function. Post creation, the table is accessible throughout SparkSession by using sql() method. When the SparkSession gets terminated, the temporary table will be dropped.
```pyspark
groupByGender = spark.sql("SELECT Gender, count(*) as Gender_Count from STUDENTS group by Gender")

groupByGender.show()
```
#### How can you inner join two DataFrames?
```pyspark
emp_dept_df = empDF.join(deptDF,empDF.empdept_id == deptDF.dept_id,"inner").show(truncate=False)
```
#### PySpark Streaming

PySpark Streaming is scalable, fault-tolerant, high throughput based processing streaming system that supports streaming as well as batch loads for supporting real-time data from data sources like TCP Socket, S3, Kafka, Twitter, file system folders etc. The processed data can be sent to live dashboards, Kafka, databases, HDFS etc. If any RDD partition is lost, then that partition can be recomputed using operations lineage from the original fault-tolerant dataset.

#### How can I increase the memory available for Apache spark executor nodes?
```pyspark
conf=SparkConf()
conf.set("spark.driver.memory", "4g") 
```
#### Spark Performance Tuning

Spark Performance tuning is a process to improve the performance of the Spark and PySpark applications by adjusting and optimizing system resources (CPU cores and memory). For Spark jobs, prefer using Dataset/DataFrame over RDD as Dataset and DataFrame’s includes several optimization modules to improve the performance of the Spark workloads. In PySpark use, DataFrame over RDD as Dataset’s are not supported in PySpark applications.

Since Spark DataFrame maintains the structure of the data and column types (like an RDMS table) it can handle the data better by storing and managing more efficiently. Spark Dataset/DataFrame includes Project Tungsten which optimizes Spark jobs for Memory and CPU efficiency. Tungsten is a Spark SQL component that provides increased performance by rewriting Spark operations in bytecode, at runtime.

Catalyst Optimizer is an integrated query optimizer and execution scheduler for Spark Datasets/DataFrame. Catalyst Optimizer is the place where Spark tends to improve the speed of your code execution by logically improving it. Catalyst Optimizer can perform refactoring complex queries and decides the order of your query execution by creating a rule-based and code-based optimization.

When you want to reduce the number of partitions prefer using coalesce() as it is an optimized or improved version of repartition() where the movement of the data across the partitions is lower using coalesce which ideally performs better when you dealing with bigger datasets.

Most of the Spark jobs run as a pipeline where one Spark job writes data into a File and another Spark jobs read the data, process it, and writes to another file for another Spark job to pick up. When you have such use case, prefer writing an intermediate file in Serialized and optimized formats like Avro, Parquet e.t.c, any transformations on these formats performs better than text, CSV, and JSON.

Apache Parquet is a columnar file format that provides optimizations to speed up queries and is a far more efficient file format than CSV or JSON.

Apache Avro is an open-source, row-based, data serialization and data exchange framework for Hadoop projects, originally developed by databricks as an open-source library that supports reading and writing data in Avro file format. it is mostly used in Apache Spark especially for Kafka-based data pipelines. When Avro data is stored in a file, its schema is stored with it, so that files may be processed later by any program.

Try to avoid Spark/PySpark UDF’s at any cost and use when existing Spark built-in functions are not available for use. UDF’s are a black box to Spark hence it can’t apply optimization and you will lose all the optimization Spark does on Dataframe/Dataset. When possible you should use Spark SQL built-in functions as these functions provide optimization.

Using cache() and persist() methods, Spark provides an optimization mechanism to store the intermediate computation of a Spark DataFrame so they can be reused in subsequent actions.

When you persist a dataset, each node stores it’s partitioned data in memory and reuses them in other actions on that dataset. And Spark’s persisted data on nodes are fault-tolerant meaning if any partition of a Dataset is lost, it will automatically be recomputed using the original transformations that created it.

When caching use in-memory columnar format, By tuning the batchSize property you can also improve Spark performance.
```pyspark
df.where(col("State") === "PR").cache()

spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", true)
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize",10000)
```
#### Shuffling

Shuffling is a mechanism Spark uses to redistribute the data across different executors and even across machines. Spark shuffling triggers when we perform certain transformation operations like gropByKey(), reducebyKey(), join() on RDD and DataFrame. We cannot completely avoid shuffle operations in but when possible try to reduce the number of shuffle operations removed any unused operations.

#### Basics of Apache Spark Configuration Settings

In Spark, execution and storage share a unified region. When no execution memory is used, storage can acquire all available memory and vice versa. In necessary conditions, execution may evict storage until a certain limit which is set by spark.memory.storageFraction property. Beyond this limit, execution can not evict storage in any case. The default value for this property is 0.5. This tuning process is called as dynamic occupancy mechanism.

Execution memory is used to store temporary data in the shuffle, join, aggregation, sort, etc. Note that, the data manipulations are actually handled in this part. On the other hand, storage memory is used to store caching and broadcasting data. Execution memory has priority over storage memory as expected. The execution of a task is more important than cached data.

spark.executor.instances: Number of executors for the spark application.

spark.executor.memory: Amount of memory to use for each executor that runs the task.

spark.executor.cores: Number of concurrent tasks an executor can run.

spark.driver.memory: Amount of memory to use for the driver.

spark.driver.cores: Number of virtual cores to use for the driver process.

spark.sql.shuffle.partitions: Number of partitions to use when shuffling data for joins or aggregations.

spark.default.parallelism: Default number of partitions in resilient distributed datasets (RDDs) returned by transformations like join and aggregations.

#### Spark Broadcast Variables

In Spark RDD and DataFrame, Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks. Instead of sending this data along with every task, spark distributes broadcast variables to the machine using efficient broadcast algorithms to reduce communication costs.

When you run a Spark RDD, DataFrame jobs that has the Broadcast variables defined and used, Spark does the following.

Spark breaks the job into stages that have distributed shuffling and actions are executed with in the stage.

Later Stages are also broken into tasks.

Spark broadcasts the common data (reusable) needed by tasks within each stage.

The broadcasted data is cache in serialized format and deserialized before executing each task.

You should be creating and using broadcast variables for data that shared across multiple stages and tasks.
```pyspark
states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)
```
#### How to force spark to avoid Dataset re-computation?

We can use spark cache to cache the dataset so that it can be used in multiple computations without creating it multiple times.

#### Aggregate vs Reduce By

Aggregate operation allows to specify a combiner function (to reduce the amount of data sent through the shuffle), which is different to Reduce By.

PySpark reduceByKey() transformation is used to merge the values of each key using an associative reduce function on PySpark RDD.
```pyspark
rdd2=rdd.reduceByKey(lambda a,b: a+b)
for element in rdd2.collect():
    print(element)
```
#### Cache vs Persist

Both caching and persisting are used to save the Spark RDD, Dataframe, and Dataset's. But, the difference is, RDD cache() method default saves it to memory (MEMORY_ONLY) whereas persist() method is used to store it to the user-defined storage level.

#### Narrow vs Wide transformation

Narrow transformation - doesn't require the data to be shuffled across the partitions. for example, Map, filter, flat Map, mapPartitions, etc.

Wide transformation - requires the data to be shuffled, for example, reduceByKey, intersection, Distinct, GroupByKey, Join, Repartition, Coalesce.

#### Difference between SortAggregate & HashAggregate

In SortAggregate, first data is sorted based on grouping columns and then aggregated. This is time-consuming process and it increases significantly as the data grows. 

In HashAggregate, no sorting of data takes place before aggregation and time consumed will be relatively less. 

But HashAggregate takes additional memory (from off-heap) for execution but SortAggregate doesn't.

#### Data Purging Vs Data Deletion

Data Purging means deleting the records or data from the main/primary location where the data is stored (DB). But then it maintains a copy of it (kind of back-up) in an archived folder, from where it can be recovered.

Data Deletion: Here it does not maintain a back up of the data in an archived folder, due to which once deleted, it is permanently lost. Data cannot be recovered once deleted.

#### How to calculate cluster memory in Spark?

Consider, 10 Node Cluster ( 1 Master Node, 9 Worker Nodes), Each 16VCores and 64GB RAM

spark.excutor.cores  = 5

5 virtual cores for each executor is ideal to achieve optimal results in any sized cluster.(Recommended)

No. of executors/instance = (total number of virtual cores per instance – 1)/spark.executors.cores

[1 reserve it for the Hadoop daemons]

= (16-1)/5

=15/5 = 3

spark.executor.instances = (number of executors per instance * number of core instances) – 1

[1 for driver]

= (3 * 9) – 1

= 27-1 = 26

Total executor memory = total RAM per instance / number of executors per instance

= 63/3 = 21

Leave 1 GB for the Hadoop daemons.

This total executor memory includes both executor memory and overheap in the ratio of 90% and 10%.

So,

spark.executor.memory = 21 * 0.90 = 19GB

spark.yarn.executor.memoryOverhead = 21 * 0.10 = 2GB

spark.driver.memory = spark.executors.memory

spark.driver.cores= spark.executors.cores
