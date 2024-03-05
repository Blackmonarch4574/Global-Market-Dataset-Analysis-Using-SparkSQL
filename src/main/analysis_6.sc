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

// Assuming 'df' is your DataFrame with columns: 'Order Date' (as DateType), 'Product Name', and 'Profit'

// Extract year from Order Date
val dfWithYear = df.withColumn("Year", year(col("Order_Date")))

// Group by Product Name and Year, calculate total profit
val profitByProduct = dfWithYear
  .groupBy("Year", "Product_Name")
  .agg(sum("Profit").alias("TotalProfit"))

// Window specification to rank products within each year
val windowSpec = Window.partitionBy("Year").orderBy(desc("TotalProfit"))

// Rank products within each year based on total profit
val rankedProfitByProduct = profitByProduct
  .withColumn("Rank", dense_rank().over(windowSpec))

// Filter for the top 5 profitable products for each year
val top5ProfitByYear = rankedProfitByProduct
  .filter(col("Rank") <= 5)
  .orderBy(col("Year").desc, col("TotalProfit").desc)

top5ProfitByYear.show()

val outputPath = "/home/likith/DBMS/outputs/result_6.csv"

top5ProfitByYear.write.option("header", "true").csv(outputPath)