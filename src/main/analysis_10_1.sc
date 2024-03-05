
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

val uniqueCountryCount = df.select("Country").distinct().count()
println(s"Count of unique countries: $uniqueCountryCount")

val dfWithYear = df.withColumn("Year", year(col("Order_Date")))

val profitByCountryYear = dfWithYear.groupBy("Year", "Country")
  .agg(sum("Profit").alias("TotalProfit"))
  .orderBy(col("Year"), col("TotalProfit").desc)
profitByCountryYear.show(50)

val specificYear = 2011
val topCountryByProfitInYear = profitByCountryYear.filter(col("Year") === specificYear)
  .select("Year", "Country", "TotalProfit").limit(1)

topCountryByProfitInYear.show()


val specificYear = 2012
val topCountryByProfitInYear = profitByCountryYear.filter(col("Year") === specificYear)
  .select("Year", "Country", "TotalProfit").limit(3)

topCountryByProfitInYear.show()
val specificYear = 2013
val topCountryByProfitInYear = profitByCountryYear.filter(col("Year") === specificYear)
  .select("Year", "Country", "TotalProfit").limit(3)

topCountryByProfitInYear.show()

val specificYear = 2014
val topCountryByProfitInYear = profitByCountryYear.filter(col("Year") === specificYear)
  .select("Year", "Country", "TotalProfit").limit(3)

topCountryByProfitInYear.show()