import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


val spark = SparkSession.builder().master("local[*]")
  .appName("store_analysis")
  .getOrCreate()

val df = spark.read
  .format("jdbc")
  .option("driver","com.mysql.cj.jdbc.Driver")
  .option("url", "jdbc:mysql://localhost:3306/market")
  .option("dbtable", "global_market")
  .option("user", "root")
  .option("password", "Likki@")
  .load()

// Assuming df is your DataFrame with columns: Order Date (as DateType), Category, Sub-Category, Sales

// Define the specific category and subcategory
val specificCategory = "Furniture"
val specificSubCategory = "Chairs" // Replace this with the desired subcategory name

// Filter data for the specific category and subcategory
val filteredData = df.filter(col("Category") === specificCategory && col("Sub_Category") === specificSubCategory)

// Extracting month and year from Order Date
val dfWithMonthYear = filteredData
  .withColumn("Month", month(col("Order_Date")))

// Grouping by Year, Month, and summing up the Sales
val monthlySalesBySubCategory = dfWithMonthYear
  .groupBy("Month")
  .agg(sum("Sales").alias("TotalSales"))
  .orderBy("Month")

// Displaying the Monthly Sales for the Specific Subcategory within the Category
monthlySalesBySubCategory.show()

val subcategoryHighestSalesPerMonth = df
  .filter(col("Category") === specificCategory)
  .withColumn("Month", month(col("Order_Date")))
  .groupBy("Month", "Sub_Category")
  .agg(sum("Sales").alias("TotalSales"))
  .groupBy("Month")
  .agg(max(struct(col("TotalSales"), col("Sub_Category"))).as("MaxSalesPerMonth"))
  .select(col("Month"), col("MaxSalesPerMonth.Sub_Category").alias("Sub_Category"), col("MaxSalesPerMonth.TotalSales").alias("TotalSales"))
  .orderBy("Month")

subcategoryHighestSalesPerMonth.show()

val outputPath = "/home/likith/DBMS/outputs/result_2.csv"

subcategoryHighestSalesPerMonth.write.option("header", "true").csv(outputPath)