import DroneReport.Identity
import DroneReport.Drone
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import net.liftweb.json._
import net.liftweb.json.Serialization.write





object Main {
	def main(args: Array[String]): Unit = {
    val producerThread = new Thread {
      override def run: Unit = {
         ProducerDrone.run()
      }
    }

    val consumerThread = new Thread {
      override def run: Unit = {
         ConsumerDrone.run()
      }
    }

    val consumerToCsvThread = new Thread {
      override def run: Unit = {
         ConsumerToCSV.run()
      }
    }

		producerThread.start()
		consumerThread.start()
		consumerToCsvThread.start()
	}
}
