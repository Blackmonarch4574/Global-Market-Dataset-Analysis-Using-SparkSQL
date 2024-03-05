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

// Assuming df is your DataFrame with columns: Order Date (as DateType), Sales, Profit

// Extracting month and year from Order Date
val dfWithMonthYear = df
  .withColumn("Month", month(col("Order_Date")))
  .withColumn("Year", year(col("Order_Date")))

// Grouping by Year and Month to calculate total sales and profit
val monthlyTrends = dfWithMonthYear
  .groupBy("Year", "Month")
  .agg(sum("Sales").alias("TotalSales"))
  .orderBy("Year", "Month")

// Find peak sales/profit months for every year
val peakMonths = monthlyTrends.join(
      monthlyTrends.groupBy("Year")
        .agg(max("TotalSales").alias("MaxSales")),
      Seq("Year")
  )
  .filter(col("TotalSales") === col("MaxSales"))
  .select("Year", "Month", "TotalSales").orderBy("Year")

// Displaying Results
monthlyTrends.show() // Monthly trends
peakMonths.show()    // Peak months for every year

val outputPath = "/home/likith/DBMS/outputs/result_7.csv"

monthlyTrends.write.option("header", "true").csv(outputPath)