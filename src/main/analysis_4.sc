import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.expressions._

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



// Define your segmentation criteria based on customer behavior or characteristics
val segmentationCriteria = df.groupBy("Customer_ID","Customer_Name")
  .agg(
    sum("Sales").alias("TotalSales"),
    sum("Profit").alias("TotalProfit"),
    count("Order_ID").alias("OrderCount"),
    avg("Sales").alias("AvgSalesByCustomer"),
    avg("Profit").alias("AvgProfitByCustomer")
  )

// Example segmentation based on total sales and profit
val segments = when(
  col("AvgSalesByCustomer") >= 400 && col("AvgProfitByCustomer") >= 50, "High Value Customer"
).otherwise(
  when(col("AvgSalesByCustomer") >= 200 && col("AvgProfitByCustomer") >= 20, "Medium Value Customer")
    .otherwise("Low Value Customer")
)

val segmentedCustomers = segmentationCriteria.withColumn("Segment", segments)

segmentedCustomers.show(50)

val outputPath = "/home/likith/DBMS/outputs/result_4.csv"

segmentedCustomers.write.option("header", "true").csv(outputPath)