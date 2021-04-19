import DroneReport.Identity
import DroneReport.Drone
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import net.liftweb.json._
import net.liftweb.json.Serialization.write

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.concurrent.TimeUnit

object Main {
	def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val n_tasks = 5
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

    TimeUnit.MILLISECONDS.sleep(n_tasks*milliseconds_per_task+(milliseconds_per_task*10).toInt)

		Spark.firstLook()
    Spark.nightCrime()
    Spark.averagePeacescorePerTimePeriod()
    Spark.crimesInParis()
    Spark.riotCountPerTimePeriod()
	}
}
