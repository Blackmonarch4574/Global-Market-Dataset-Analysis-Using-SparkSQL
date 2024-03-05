import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import matplotlib.pyplot as plt

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


// Assuming 'df' is your DataFrame with columns: 'Order Date' (as DateType), 'Ship Date' (as DateType), and 'Country'

// Calculate delivery time in days
val dfWithDeliveryTime = df.withColumn("DeliveryTime", datediff($"Ship Date", $"Order Date"))

// Group by country and calculate the average delivery time
val avgDeliveryTimeByCountry = dfWithDeliveryTime
  .groupBy("Country")
  .agg(avg("DeliveryTime").alias("AvgDeliveryTime"))
  .orderBy("Country")

// Convert DataFrame to Pandas DataFrame for visualization in Python
val avgDeliveryTimePandas = avgDeliveryTimeByCountry.toPandas()

// Convert to Python and visualize using Matplotlib or any plotting library
// Assuming you have a Python environment with Matplotlib installed


// Plotting the bar plot
plt.figure(figsize=(10, 6))
plt.bar(avgDeliveryTimePandas["Country"], avgDeliveryTimePandas["AvgDeliveryTime"], color='skyblue')
plt.xlabel('Country')
plt.ylabel('Average Delivery Time (Days)')
plt.title('Average Delivery Time Across Countries')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()