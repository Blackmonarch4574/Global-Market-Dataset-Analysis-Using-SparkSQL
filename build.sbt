
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "store_analysis"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"

// Only include spark-sql, not spark-core and spark-streaming separately
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.0" % "provided"


// Remove the duplicate Scala Swing dependency
libraryDependencies += "org.scala-lang.modules" %% "scala-swing" % "3.0.0"

// MySQL connector
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"

// Breeze and Breeze-viz
libraryDependencies ++= Seq(

  "org.scalanlp" %% "breeze" % "2.1.0",
  "org.scalanlp" %% "breeze-viz" % "2.1.0"
)

// Splot library
//libraryDependencies += "com.github.piotr-kalanski" %% "splot" % "0.2.0"


// https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.1"


// https://mvnrepository.com/artifact/org.mongodb/mongodb-driver-sync
libraryDependencies += "org.mongodb" % "mongodb-driver-sync" % "4.11.1"

//libraryDependencies += "org.mongodb"

//libraryDependencies += "org.plotly-scala" %% "plotly-almond" % "0.10.2"


// https://mvnrepository.com/artifact/org.plotly-scala/plotly-core
libraryDependencies += "org.plotly-scala" %% "plotly-core" % "0.8.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.0" % "provided"

