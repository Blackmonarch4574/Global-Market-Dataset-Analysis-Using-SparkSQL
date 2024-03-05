import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

val spark = SparkSession.builder().master("local[*]")
  .appName("BDMS Project")
  .getOrCreate()

val df = spark.read
  .format("jdbc")
  .option("driver","com.mysql.cj.jdbc.Driver")
  .option("url", "jdbc:mysql://localhost:3306/market")
  .option("dbtable", "global_market")
  .option("user", "root")
  .option("password", "Likki@")
  .load()


// Extracting the month from the 'Order Date'
val dfWithMonth = df.withColumn("Month", month(col("Order_Date")))

// Define seasons based on month ranges
val seasonCategories = dfWithMonth.withColumn("Season",
  when(col("Month").between(3, 5), lit("Spring"))
    .when(col("Month").between(6, 8), lit("Summer"))
    .when(col("Month").between(9, 11), lit("Autumn"))
    .otherwise(lit("Winter"))
)

// Grouping by Season and Category to find the most ordered category in each season
val  totalordercategoryseason = seasonCategories.groupBy("Season", "Category")
  .agg(count("Order_ID").alias("TotalOrders"))
  .orderBy(col("Season"), col("TotalOrders").desc)



val mostOrderedCategoryBySeason = totalordercategoryseason.groupBy("Season").agg(
  first("Category").alias("MostOrderedCategory"),
  first("TotalOrders").alias("MaxOrders")
)

totalordercategoryseason.show()
mostOrderedCategoryBySeason.show()

val outputPath = "/home/likith/DBMS/outputs/result_8.csv"

totalordercategoryseason.write.option("header", "true").csv(outputPath)