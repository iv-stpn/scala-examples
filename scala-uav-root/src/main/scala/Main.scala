import DroneReport.Identity
import DroneReport.Drone
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import net.liftweb.json._
import net.liftweb.json.Serialization.write

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark

import java.util.concurrent.TimeUnit

object Main {
	def main(args: Array[String]): Unit = {
    val n_tasks = 30
    val milliseconds_per_task = 1000

    val producerThread = new Thread {
      override def run: Unit = {
         ProducerDrone.run(n_tasks, milliseconds_per_task)
      }
    }

    val consumerThread = new Thread {
      override def run: Unit = {
         ConsumerDrone.run(n_tasks*milliseconds_per_task, milliseconds_per_task)
      }
    }

    val consumerToCsvThread = new Thread {
      override def run: Unit = {
         ConsumerToCSV.run(n_tasks*milliseconds_per_task, milliseconds_per_task)
      }
    }
    producerThread.start()
		consumerThread.start()
		consumerToCsvThread.start()

    println()
    println()
    println()
    println(n_tasks*milliseconds_per_task+(milliseconds_per_task*2.5).toInt)
    println()
    println()
    println()

    TimeUnit.MILLISECONDS.sleep(n_tasks*milliseconds_per_task+(milliseconds_per_task*2.5).toInt)

		val pathToCSV = "testdd.csv"

		val sparkConfig = new SparkConf()
			.setMaster("local")
			.setAppName("SPARK SQL app conf")

		val spark = SparkSession
			.builder()
			.appName("SPARK SQL app sess")
			.config(sparkConfig)
			.getOrCreate

		val df = spark.read
			.option("header", "true")
			.option("inferSchema", "true")
			.csv(pathToCSV)

		val r1 = spark.sql("SELECT * FROM csv.`" + pathToCSV + "`")
		val r2 = spark.sql("SELECT * FROM csv.`" + pathToCSV + "` WHERE surround = Broussole")
		val r3 = spark.sql("SELECT * FROM csv.`" + pathToCSV + "` WHERE words = peur")
		val r4 = spark.sql("SELECT * FROM csv.`" + pathToCSV + "` WHERE words = aime")
		print(r1, "\n", r2, "\n", r3, "\n", r4, "\n")
	}
}
