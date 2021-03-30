object DroneReport {

  case class Identity(
                       firstname: String,
                       lastname: String,
                       adress: String,
                       peacescore: Int
                     )

  case class Drone(
                    id : Int,
                    lat_location : Double,
                    long_location : Double,
                    words : List[String],
                    surround : List[Identity]
                  )

}
