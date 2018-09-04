package com.bitcoin.load

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.parsing.json._


class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }

object M extends CC[Map[String, Any]]
object L extends CC[List[Any]]
object S extends CC[String]

case class DayPrice(price: String, time:String)

/**
  * Load the BitCoin price of last 365 days to MongoDB.
  */
object LoadPrices {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", (new java.io.File(".")).getCanonicalPath())

    val result = getBitCoinPricesHistory()
    val prices = readPrices(result)
    saveToDB(prices);
  }

  def getBitCoinPricesHistory(): String = {
    //Read the last 365 days BitCoin prices from Coinbase.
    val url = "https://www.coinbase.com/api/v2/prices/BTC-USD/historic?period=year";
    return scala.io.Source.fromURL(url).mkString
  }

  def readPrices(result:String): List[DayPrice] = {
    //Parse the JSON response from Coinbase to get Price of time of all 365 days.
    val prices = for {
      Some(M(map)) <- List(JSON.parseFull(result))
      M(data) = map("data")
      L(prices) = data("prices")
      M(prices) <- prices
      S(price) = prices("price")
      S(time) = prices("time")
    } yield {
      DayPrice(price, time)
    }
    return prices;
  }

  def getSparkSession(): SparkSession = {
    //All these properties has to be moved to spark properties file which will be picked when the Job is submitted to Cluster using spark-submit.

    return SparkSession.builder()
      .master("local")
      .appName("BitCoinPricesLoader")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bcp.prices")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/bcp.prices")
      .getOrCreate()
  }

  def getDocuments(prices:List[DayPrice]): List[Document] = {
    println(prices)
    //Convert the prices to BSON Document to save to MongoDB.
    return prices.map(dp => Document.parse("{\"price\":" + dp.price + ",\"time\":ISODate(\"" + dp.time + "\")}"))
  }

  def saveToDB(prices:List[DayPrice]): Unit = {
    //Save predicted prices to MongoDB - bcp.prices.
    val documents = getDocuments(prices)
    //println(documents)
    val spark = getSparkSession()
    val rdd = spark.sparkContext.parallelize(documents)
    MongoSpark.save(rdd);
    spark.close()
  }

}
