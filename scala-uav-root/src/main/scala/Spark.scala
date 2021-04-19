import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Spark {
  Logger.getRootLogger().setLevel(Level.OFF)


  val file = "DB.csv"

  // create spark configuration and spark context: the Spark context is the entry point in Spark.
  // It represents the connexion to Spark and it is the place where you can configure the common properties
  // like the app name, the master url, memories allocation...
  val conf = new SparkConf()
    .setAppName("DroneAnalyser")
    .setMaster("local[*]")

  //Logger.getLogger("org").setLevel(Level.OFF)
  //Logger.getLogger("akka").setLevel(Level.OFF)
  val ss = SparkSession.builder()
    .config(conf)
    .getOrCreate()
  Logger.getRootLogger().setLevel(Level.ERROR)

  //ss.sparkContext.setLogLevel("ERROR")
  
  val df = ss.read.options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")).csv(file)

  def firstLook(): Unit = {
    println()
    println()
    df.show()
    println()
    println()
  }

  def nightCrime(): Unit = {

    val nightCrime = df.select("peacescore", "drone_time").filter("peacescore > 50").filter(hour(col("drone_time")) < 6 || hour(col("drone_time")) > 22).count()
    val allCrime = df.filter("peacescore > 50").count()
    val nightCrimeRatio = nightCrime.toDouble/allCrime.toDouble
    println()
    println()
    //df.select(hour(col("drone_time"))).show()
    println("Night crime ratio: " + (nightCrimeRatio-(nightCrimeRatio % 0.01)))
    println()
    println()
  }

  def averagePeacescorePerTimePeriod(): Unit = {
    val averageDf = df.select(col("peacescore"), hour(col("drone_time")).alias("hour"), date_format(col("drone_time"), "E").alias("dayOfWeek"),
                                            month(col("drone_time")).alias("month"), quarter(col("drone_time")).alias("quarter"))
    
    val hourAvg = averageDf.groupBy("hour").agg(avg("peacescore") as "avg_hour")
    val dayOfWeekAvg = averageDf.groupBy("dayOfWeek").agg(avg("peacescore") as "avg_dayOfWeek")
    val monthAvg = averageDf.groupBy("month").agg(avg("peacescore") as "avg_month")
    val quarterAvg = averageDf.groupBy("quarter").agg(avg("peacescore") as "avg_quarter")


    hourAvg.show()
    dayOfWeekAvg.show()
    monthAvg.show()
    quarterAvg.show()
    //println("hourAvg: " + hourAvg.first().getDouble(0) + ", dayOfWeekAvg: " + dayOfWeekAvg.first().getDouble(0) 
    //        + ", monthAvg: " + monthAvg.first().getDouble(0) + ", quarterAvg: " + quarterAvg.first().getDouble(0))

    /*val average = df.select(mean(df("peacescore"))).first().getDouble(0)
    println()
    println()
    println("Average of peacescore in lands: " + average)
    println()
    println()*/
  }

  def riotCountPerTimePeriod(): Unit = {
    val countDf = df.select(col("peacescore"), hour(col("drone_time")).alias("hour"), date_format(col("drone_time"), "E").alias("dayOfWeek"),
                                            month(col("drone_time")).alias("month"), quarter(col("drone_time")).alias("quarter"))
    
    val hourCount = countDf.where(col("peacescore") > 50).groupBy("hour").agg(count("peacescore") as "count_hour")
    val dayOfWeekCount = countDf.where(col("peacescore") > 50).groupBy("dayOfWeek").agg(count("peacescore") as "count_dayOfWeek")
    val monthCount = countDf.where(col("peacescore") > 50).groupBy("month").agg(count("peacescore") as "count_month")
    val quarterCount = countDf.where(col("peacescore") > 50).groupBy("quarter").agg(count("peacescore") as "count_quarter")

    hourCount.show()
    dayOfWeekCount.show()
    monthCount.show()
    quarterCount.show()

  }


  def crimesInParis(): Unit = {
    val paris = df.select("firstname", "lastname", "address", "peacescore").where(col("lat_location") > 48.8 && col("lat_location") < 48.9 && col("long_location") > 2.3 && col("long_location") < 2.4 && col("peacescore") > 50)
    println()
    println()
    println("Paris: ")
    paris.show()
    println()
    println()
  }
}