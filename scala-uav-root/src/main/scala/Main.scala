import DroneReport.Identity
import DroneReport.Drone
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import net.liftweb.json._
import net.liftweb.json.Serialization.write

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark

object Main {



	def main(args: Array[String]): Unit = {


		/* val random_name = List("Xinrui", "Haoyuan", "Yue", "Sujin", "Chaoshi", "Lihe", "Xinyu", "Qixian", "Songyan", "Cao","Paul", "Omar", "Adrien", "Ivan", "Louis", "Julien", "Marion", "Claire", "Charles", "Julie","Alexandre", "Diane", "Capucine", "Victor", "Antoine")
     val random_lastname = List("Boulanger", "Charpentier", "Dorffer", "Stepanian", "Allouache", "Hammoud", "Petit", "Martin", "Bernard", "Robert", "Richard","Wang", "Li", "Zhang'", "Liu", "Chen", "Yang", "Zhao", "Huang", "Wu", "Zhou", "Yuan")
     val random_adress = List("Arbres", "Poissons", "Champs", "Martyrs", "Hongrois", "Concombres", "Italiens", "Espagnols", "Français", "Marcassins")
     val random_gene = scala.util.Random

     val name = Identity(random_name(random_gene.nextInt(random_name.length)),
                         random_lastname(random_gene.nextInt(random_lastname.length)),
                         random_gene.nextInt(200).toString() + " Rue des " + random_adress(random_gene.nextInt(random_adress.length)),
                         random_gene.nextInt(100))

     val words = List("Bonjour", "Comment", "Bien", "Attentat", "Paris", "Vive notre grande patrie", "La voix du peuple sera entendue", "A l'aide !", "La nouvelle révolution",
       "J'ai peur", "Attention !", "Pourquoi ?", "Je t'aime", "Soyons unis", "Ensemble", "Seul",
       "Les damnés de la terre", "La domination des élites", "La grande invasion de l'Est",
       "Les Etats-Unis ont corrompu notre nation", "Ou est notre sauveteur américain ?",
       "Les peacewatchers arrive", "Gloire au nouvel ordre mondial")

     val listwords = (((words(random_gene.nextInt(words.length)).split(" ") ++ words(random_gene.nextInt(words.length)).split(" ")) ++ words(random_gene.nextInt(words.length)).split(" ")) ++ words(random_gene.nextInt(words.length)).split(" ")).toList

     val reporttest = Drone(randomUUID().toString(), 47 + random_gene.nextFloat(), 2 + random_gene.nextFloat() , listwords,name)
     println("Report test ", reporttest)

     implicit val formats = DefaultFormats
     val jsonString = write(reporttest)
     println(jsonString)*/

		/*val gson = new Gson
    val gsons = gson.toJson(reporttest)
    print(gsons)*/
		/*val thread = new Thread {
      override def run: Unit = {
         ConsumerDrone.test()

      }
    }*/
		//thread.start()
		ProducerDrone.test()
		ConsumerToCSV.test()

		val pathToCSV = "/home/omar/Bureau/scala-examples/scala-uav-root/testdd.csv"

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
