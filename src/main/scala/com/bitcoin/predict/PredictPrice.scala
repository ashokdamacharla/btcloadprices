package com.bitcoin.predict

import java.text.SimpleDateFormat
import java.util.Date

import org.joda.time.DateTime
import com.mongodb.spark.MongoSpark
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.Random

/**
  * Load the BitCoin price Predictions for next 365 days to MongoDB.
  */
object PredictPrice {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", (new java.io.File(".")).getCanonicalPath())

    val list = readAllFromDB()
    val prices: List[java.lang.Double] = list.map(_._1)
    val predictions = predict(prices)
    val dayPrices = updateTheDates(list, predictions);
    saveToDB(dayPrices);

    //println(result)
    //println("Test");
  }

  def predict(prices: List[java.lang.Double]): List[(Double, Double)] = {
    //Predict the BitCoin prices for next 365 days.
    val spark = getSparkSession("")

    val parsedData = prices.map{line =>
      val c = Vectors.dense(line,(line+Random.nextInt%1000))
      LabeledPoint(line, c)
    }
    val rddData = spark.sparkContext.parallelize(parsedData)

    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(rddData, numIterations, stepSize)

    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //valuesAndPreds.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))

    spark.close()
    return valuesAndPreds;
  }

  def updateTheDates(dataList:List[(java.lang.Double, Date)], predictions:List[(Double, Double)]): List[(Date, Double)] = {
    //The dates are updated for the predicted prices
    // based on their order of values of the list that is fetched from DB.
    val predictedPriceDates = for {
      (a, i) <- dataList.zipWithIndex
    } yield (
      (addDays(a._2, 365), predictions(i)._2)
    )
    return predictedPriceDates;
  }

  def readAllFromDB(): List[(java.lang.Double, Date)] = {
    //Read last 365 days BitCoin prices from MongoDB.
    val spark = getSparkSession("bcp.prices")
    val customRdd = MongoSpark.load(spark.sparkContext)

    //println(customRdd.count)
    val list: List[(java.lang.Double, Date)] = customRdd.map(price =>
      (price.getDouble("price"), price.getDate("time"))
    ).collect().toList
    //println(list)
    spark.close()
    return list;
  }

  def saveToDB(dPrices:List[(Date, Double)]): Unit = {
    //Save predicted prices to MongoDB - bcp.predictions.
    val documents = getDocuments(dPrices)
    //println(documents)
    val spark = getSparkSession("bcp.predictions")
    val rdd = spark.sparkContext.parallelize(documents)
    MongoSpark.save(rdd);
    spark.close()
  }

  def getDocuments(dPrices:List[(Date, Double)]): List[Document] = {
    //Convert the prices to BSON Document to save to MongoDB.
    return dPrices.map(dp => Document.parse("{\"price\":" + dp._2 + ",\"time\":ISODate(\"" + formatDate(dp._1) + "\")}"))
  }

  def formatDate(date:Date): String = {
    //Format the date to store to MongoDB.
    val format = new SimpleDateFormat("yyyy-MM-dd")
    return format.format(date)
  }

  def addDays(date:Date, days:Int):Date = {
    //Increase the date to the given no of days.
    val dateTime = new DateTime(date)
    return dateTime.plusDays(days).toDate
  }

  def getSparkSession(collection:String): SparkSession = {
    //All these properties has to be moved to spark properties file which will be picked when the Job is submitted to Cluster using spark-submit.
    val spark = SparkSession.builder()
      .master("local")
      .appName("BitCoinPricesLoader")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/"+collection)
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/"+collection)
      .getOrCreate()
    return spark;
  }

}
