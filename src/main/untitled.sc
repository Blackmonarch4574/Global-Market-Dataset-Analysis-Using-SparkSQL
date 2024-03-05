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

df.printSchema()

//root
//|-- Row_ID: integer (nullable = true)
//|-- Order_ID: string (nullable = true)
//|-- Order_Date: date (nullable = true)
//|-- Ship_Date: date (nullable = true)
//|-- Ship_Mode: string (nullable = true)
//|-- Customer_ID: string (nullable = true)
//|-- Customer_Name: string (nullable = true)
//|-- Segment: string (nullable = true)
//|-- City: string (nullable = true)
//|-- State: string (nullable = true)
//|-- Country: string (nullable = true)
//|-- Postal_Code: integer (nullable = true)
//|-- Market: string (nullable = true)
//|-- Region: string (nullable = true)
//|-- Product_ID: string (nullable = true)
//|-- Category: string (nullable = true)
//|-- Sub_Category: string (nullable = true)
//|-- Product_Name: string (nullable = true)
//|-- Sales: decimal(10,2) (nullable = true)
//|-- Quantity: integer (nullable = true)
//|-- Discount: decimal(4,2) (nullable = true)
//|-- Profit: decimal(10,2) (nullable = true)
//|-- Shipping_Cost: decimal(10,2) (nullable = true)
//|-- Order_Priority: string (nullable = true)