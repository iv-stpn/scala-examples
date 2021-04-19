import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Spark {

  val file = "DB.csv"
  def loadData(): DataFrame = {
    // create spark configuration and spark context: the Spark context is the entry point in Spark.
    // It represents the connexion to Spark and it is the place where you can configure the common properties
    // like the app name, the master url, memories allocation...
    val conf = new SparkConf()
      .setAppName("Wordcount")
      .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    ss.read.options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")).csv(file)
  }

  val df = loadData()

  def firstLook(): Unit = {
    df.show()
  }

  def peacescoreCount(): Unit = {
    df.select("address").show()
    val alertnumber = df.select("peacescore").filter("peacescore>50").count()
    println("Total count of alert " + alertnumber)

  }

  def averagePeacesocre(): Unit = {
    val average = df.select(mean(col("peacescore")))
    println("Average of peacescore in lands" + average)
  }

  def crimesInParis(): Unit = {
    val paris = df.select("lastname").where(col("lat_location") > 48.8 && col("lat_location") < 48.9 && col("long_location") > 2.3 && col("long_location") < 2.4 && col("peacescore") > 50)
    println(paris)
  }
}