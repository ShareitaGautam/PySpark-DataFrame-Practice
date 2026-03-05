# Day19 PySpark Practice
# This file contains simple PySpark commands based on class rough notes.
# Each step has comments explaining what the command does.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, lit, array_contains

# Create Spark session
# This starts Spark so we can create DataFrames
spark = SparkSession.builder.appName("Day19Practice").getOrCreate()

# ---------------------------------------------------------
# 1. Nested Structure Example
# ---------------------------------------------------------

# Data where name is a STRUCT (firstname, middlename, lastname)
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
]

# Schema definition for nested structure
structureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

# Create DataFrame using schema
df2 = spark.createDataFrame(data=structureData, schema=structureSchema)

# Print structure of dataframe
df2.printSchema()

# show() prints dataframe
# default truncate=True (long text may be cut)
df2.show()

# truncate=False shows full text
df2.show(truncate=False)

# ---------------------------------------------------------
# 2. Select Nested Columns
# ---------------------------------------------------------

# Select only name column
df2.select("name").show()

# Select firstname inside struct
df2.select("name.firstname").show()

# Select lastname inside struct
df2.select("name.lastname").show()

# ---------------------------------------------------------
# 3. Simple DataFrame
# ---------------------------------------------------------

data = [
    ("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
]

columns = ["firstname","lastname","country","state"]

# Create simple dataframe
df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)

# ---------------------------------------------------------
# 4. Different ways to select columns
# ---------------------------------------------------------

# select by column name
df.select("firstname","lastname").show()

# select single column
df.select("firstname").show()

# select using dataframe.column
df.select(df.firstname, df.lastname).show()

# select all columns
df.select("*").show()

# select using bracket notation
df.select(df["firstname"], df["lastname"]).show()

# ---------------------------------------------------------
# 5. Select columns using index
# ---------------------------------------------------------

# df.columns returns list of column names
print(df.columns)

# select first 3 columns
df.select(df.columns[:3]).show(3)

# select column index 2 and 3
df.select(df.columns[2:4]).show(3)

# ---------------------------------------------------------
# 6. DataFrame is immutable
# ---------------------------------------------------------
# DataFrames cannot be changed directly.
# Every operation creates a NEW dataframe.

# Add constant column using lit()
df_country = df.withColumn("Country", lit("USA"))

df_country.show()

# ---------------------------------------------------------
# 7. Casting column type
# ---------------------------------------------------------

data2 = [
('James','','Smith','1991-04-01','M',3000),
('Michael','Rose','','2000-05-19','M',4000),
('Robert','','Williams','1978-09-05','M',4000),
('Maria','Anne','Jones','1967-12-01','F',4000),
('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns2 = ["firstname","middlename","lastname","dob","gender","salary"]

df_salary = spark.createDataFrame(data=data2, schema=columns2)

df_salary.printSchema()

# Cast salary to Double type
ddf = df_salary.withColumn("salary", col("salary").cast("Double"))

ddf.printSchema()

# Create new column salary1 with Double type
ddf2 = df_salary.withColumn("salary1", col("salary").cast("Double"))

ddf2.show()

# ---------------------------------------------------------
# 8. Derived column example
# ---------------------------------------------------------

# Multiply salary by 100
df_mult = df_salary.withColumn("salary", col("salary") * 100)

df_mult.show()

# ---------------------------------------------------------
# 9. Array and Struct example
# ---------------------------------------------------------

data3 = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
]

schema3 = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df3 = spark.createDataFrame(data=data3, schema=schema3)

df3.printSchema()

df3.show(truncate=False)

# ---------------------------------------------------------
# 10. Filter examples
# ---------------------------------------------------------

# filter where state = OH
df3.filter(df3.state == "OH").show()

# filter where state not equal OH
df3.filter(df3.state != "OH").show()

# NOT condition
df3.filter(~(df3.state == "OH")).show()

# AND condition
df3.filter((df3.state == "OH") & (df3.gender == "M")).show()

# isin condition
li = ["OH","CA","DE"]

df3.filter(df3.state.isin(li)).show()

# NOT isin
df3.filter(~df3.state.isin(li)).show()

# startswith
df3.filter(df3.state.startswith("N")).show()

# endswith
df3.filter(df3.state.endswith("H")).show()

# contains
df3.filter(df3.state.contains("H")).show()

# ---------------------------------------------------------
# 11. Array filter
# ---------------------------------------------------------

# check if languages array contains Java
df3.filter(array_contains(df3.languages,"Java")).show()

# nested struct filter
df3.filter(df3.name.lastname == "Williams").show()

# ---------------------------------------------------------
# 12. like / rlike / ilike examples
# ---------------------------------------------------------

data4 = [
(2,"Michael Rose"),
(3,"Robert Williams"),
(4,"Rames Rose"),
(5,"Rames rose")
]

df4 = spark.createDataFrame(data=data4, schema=["id","name"])

# like is case sensitive
df4.filter(df4.name.like("%rose%")).show()

# rlike uses regex
df4.filter(df4.name.rlike("rose")).show()

# ilike is case insensitive
df4.filter(df4.name.ilike("%rose%")).show()

# Stop spark session
spark.stop()
