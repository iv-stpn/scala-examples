import DroneReport.{Drone, Identity}

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import scala.util.Random
import java.util.{Properties, Timer, TimerTask, concurrent}

object ProducerDrone extends App {
  def test(): Unit = {
    val topic = "testtopic"

    //val iden: String = identity.firstname + "-" + identity.lastname + "-(" + drone.lat_location + "," + drone.long_location + ")"
    val rand = new scala.util.Random
    val score = rand.nextInt(100)
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")

    val time = System.currentTimeMillis()
    val producer = new KafkaProducer[String, Int](props)
    val timer = new Timer()
    val task = new TimerTask {

      override def run(): Unit = {
        val random_name = List("Xinrui", "Haoyuan", "Yue", "Sujin", "Chaoshi", "Lihe", "Xinyu", "Qixian", "Songyan", "Cao","Paul", "Omar", "Adrien", "Ivan", "Louis", "Julien", "Marion", "Claire", "Charles", "Julie","Alexandre", "Diane", "Capucine", "Victor", "Antoine")
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
        println(jsonString)
        producer.send(new ProducerRecord(topic, jsonString, name.peacescore))
      }
    }

    timer.schedule(task, 1000L, 1000L)
    concurrent.TimeUnit.SECONDS.sleep(10)
    timer.cancel()
    timer.purge()
    print("Time producer " + time)
    //val record = new ProducerRecord(topic, iden, score)
    //producer.send(record)
    producer.close()
  }




}

