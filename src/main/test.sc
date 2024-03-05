import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vectors

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


val market_data_noNulls= df.na.drop()

market_data_noNulls.printSchema()

val numRows_noNulls = market_data_noNulls.count()
val numCols_noNulls = market_data_noNulls.columns.length

println(s"Number of rows after dropping null values: $numRows_noNulls")
println(s"Number of columns after dropping null values: $numCols_noNulls")