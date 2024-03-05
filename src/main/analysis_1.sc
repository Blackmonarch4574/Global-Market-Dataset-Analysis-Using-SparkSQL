//.load("/home/likith/DBMS/project/20000.csv")


import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .master("local[*]")
  .appName("BDMS Project")
  .config("spark.mongodb.input.uri", "mongodb://localhost:27017/project.store")
  .config("spark.mongodb.input.readPreference.name", "secondaryPreferred")
  .getOrCreate()

// Read data from MongoDB into a DataFrame
val df = spark.read
  .format("com.mongodb.spark.sql.DefaultSource")
  .load()

// Show the DataFrame schema and first few rows
df.printSchema()
df.show()
