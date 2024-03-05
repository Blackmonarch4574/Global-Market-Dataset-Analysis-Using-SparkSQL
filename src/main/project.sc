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

val aggDF = df.groupBy("Segment")
  .agg(avg("Profit").alias("avg"))

aggDF.show()


//val customerTenureWindow = Window.partitionBy("Customer_ID").orderBy("Order_Date")
//val dfCustomerTenure = df.withColumn("CustomerTenure",
//  datediff(last("Order_Date").over(customerTenureWindow), first("Order_Date").over(customerTenureWindow))
//).select("Customer_ID","CustomerTenure")
//dfCustomerTenure.show(50)



val outputPath = "/home/likith/DBMS/outputs/result_1.csv"

aggDF.write.option("header", "true").csv(outputPath)