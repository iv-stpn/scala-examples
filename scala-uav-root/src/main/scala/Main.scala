import DroneReport.Identity
import DroneReport.Drone
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import net.liftweb.json._
import net.liftweb.json.Serialization.write




object Main {



	def main(args: Array[String]): Unit = {

		val person = Identity("louis", "Dupont", "30 Avenue des Champs-Elysées", 42)


		val reporttest = Drone(120, 15.4856854, 3.4521684, List("Bien", "Mauvais", "Nul"),List(person))
		println("Information du rapport du drone ", reporttest)

		implicit val formats = DefaultFormats
		val jsonString = write(reporttest)
		println(jsonString)

		val thread = new Thread {
			override def run: Unit = {
				  ConsumerDrone.test()
			}
		}
		thread.start()
		ProducerDrone.test(person, jsonString)



	}

}
