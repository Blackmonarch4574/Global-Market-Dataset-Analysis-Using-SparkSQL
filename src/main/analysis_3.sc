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



// Specify the country for which you want to find the top profit-contributing customer
val specificCountry = "United States" // Replace with the desired country

// Filter data for the specific country
val dataForSpecificCountry = df.filter(col("Country") === specificCountry)

// Grouping by Country, State, City, and Customer ID to calculate total profit contributed by each customer in each city
val customerProfitByLocation = dataForSpecificCountry
  .groupBy("Country", "State", "City", "Customer_ID", "Customer_Name")
  .agg(sum("Profit").alias("TotalProfit"))
  .orderBy(col("Country"), col("State"), desc("TotalProfit"))

// Collecting the top customer in each state within the specified country
val topCustomerInEachState = customerProfitByLocation
  .groupBy("Country", "State")
  .agg(first("Customer_ID").alias("TopCustomerID"), first("Customer_Name").alias("TopCustomerName"),
    first("City").alias("City"), max("TotalProfit").alias("Profit"))
  .orderBy("Country", "State")

topCustomerInEachState.show()

val outputPath = "/home/likith/DBMS/outputs/result_3.csv"

topCustomerInEachState.write.option("header", "true").csv(outputPath)