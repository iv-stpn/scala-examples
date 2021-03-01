import scala.collection.mutable

object Main extends App {

		val person = DroneReport.Identity("louis", "Dupont", "30 Avenue des Champs-Elysées")

		val temp = mutable.HashMap((person, 50))

		val reporttest = DroneReport.Drone(120, 15.4856854, 3.4521684, List("Bien", "Mauvais", "Nul"),temp)
		println("Information du rapport du drone ", reporttest)

}
