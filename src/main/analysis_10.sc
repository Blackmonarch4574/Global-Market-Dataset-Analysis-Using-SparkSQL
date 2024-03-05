
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


// Window specification for calculating cumulative sales by product
val productCumulativeWindow = Window.partitionBy("Product_ID").orderBy("Order_Date")

// Calculate cumulative sales for each product
val dfCumulativeSales = df.withColumn("Cumulative_Sales",
  sum("Sales").over(productCumulativeWindow)
)

// Use lag function to calculate sales in the previous order
val dfWithPreviousSales = dfCumulativeSales.withColumn("Previous_Sales",
  lag("Sales", 1).over(productCumulativeWindow)
)

// Calculate sales growth for each product
val dfWithSalesGrowth = dfWithPreviousSales.withColumn("Sales_Growth",
  when(col("Previous_Sales").isNotNull, col("Sales") / col("Previous_Sales") - 1)
    .otherwise(null)
)

// Identify products with consistent sales growth
val dfConsistentGrowthProducts = dfWithSalesGrowth
  .filter(col("Sales_Growth") > 0)  // Assuming positive growth indicates consistent growth
  .select("Product_ID", "Product_Name", "Category", "Order_Date", "Sales", "Cumulative_Sales", "Sales_Growth")

dfConsistentGrowthProducts.show()

val outputPath = "/home/likith/DBMS/outputs/result_10.csv"

dfConsistentGrowthProducts.write.option("header", "true").csv(outputPath)
