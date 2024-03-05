import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.LinearRegression

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


// Assuming 'df' is your DataFrame
val dfCleaned = df.na.drop() // Handle missing values

// Convert categorical variables to numerical using StringIndexer
val indexer = new StringIndexer().setInputCol("Category").setOutputCol("CategoryIndex")
val dfIndexed = indexer.fit(dfCleaned).transform(dfCleaned)

// Assemble features into a vector
val assembler = new VectorAssembler()
  .setInputCols(Array("CategoryIndex", "Sales", "Quantity")) // Replace with your actual feature names
  .setOutputCol("features")

val dfAssembled = assembler.transform(dfIndexed)

// Check the structure and contents of the DataFrame after each transformation
df.show()
dfCleaned.show()
dfIndexed.show()
dfAssembled.show()

// Set regularization parameter to a non-zero value
val lr = new LinearRegression().setRegParam(0.01) // You can adjust the value

// Split the dataset into training and testing sets
val Array(trainData, testData) = dfAssembled.randomSplit(Array(0.8, 0.2), seed = 123)

// Train the model
val model = lr.fit(trainData)
