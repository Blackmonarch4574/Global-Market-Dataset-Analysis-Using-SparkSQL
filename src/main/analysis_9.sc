
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


//// Assuming 'df' is your DataFrame with 'Customer Name' and 'Order Date' columns
//
//// Convert 'Order Date' to a DateType
//val dfWithDate = df.withColumn("Order_Date", to_date(col("Order_Date")))
//
//// Window specification to partition by Customer Name and order by Order Date
//val windowSpec = Window.partitionBy("Customer_Name").orderBy("Order_Date")
//
//// Calculate the difference between consecutive order dates for each customer
//val result = dfWithDate.withColumn("DateDiff",
//    coalesce(datediff(col("Order_Date"), lag(col("Order_Date"), 1).over(windowSpec)), lit(0))
//  ).select("Customer_Name")
//  .filter(col("DateDiff").between(1, 3)).distinct()
//
//result.show(50)