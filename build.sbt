
name := "btcloadprices"

version := "0.1"

scalaVersion := "2.11.1"


libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.0",
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.1"
)

