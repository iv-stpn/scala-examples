import scala.collection.mutable
import scala.collection.mutable.HashMap

object DroneReport {

  case class Identity(
                     firstname: String,
                     lastname: String,
                     adress: String,
                     )

  case class Drone(
                    id : Int,
                    lat_location : Double,
                    long_location : Double,
                    words : List[String],
                    surround : mutable.HashMap[Identity, Int]
  )

}
