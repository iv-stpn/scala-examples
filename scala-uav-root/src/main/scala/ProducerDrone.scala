import DroneReport.{Drone, Identity}

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import scala.util.Random
import java.util.{Properties, Timer, TimerTask, concurrent}

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

object ProducerDrone extends App {
  def random_date(from: LocalDateTime, to: LocalDateTime): String = {
    val diff = ChronoUnit.DAYS.between(from, to)
    val random = new scala.util.Random
    from.plus(random.nextInt(diff.toInt), ChronoUnit.DAYS).plus(random.nextInt(86000), ChronoUnit.SECONDS).toString
  }

  def run(): Unit = {
    val topic = "testtopic"
    val topic_file = "testtopicfile"
    val from = LocalDateTime.of(2067, 10, 1, 0, 0, 1)
    val to = LocalDateTime.of(2075, 12, 4, 0, 0, 1)

    //val iden: String = identity.firstname + "-" + identity.lastname + "-(" + drone.lat_location + "," + drone.long_location + ")"
    val rand = new scala.util.Random
    val score = rand.nextInt(100)
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")

    val producer = new KafkaProducer[String, Int](props)
    val timer = new Timer()
    val task = new TimerTask {

      val n_tasks = 30
      val milliseconds_per_task = 1000L

      override def run(): Unit = {
        val randomizer = scala.util.Random

        val random_name = List("Xinrui", "Haoyuan", "Yue", "Sujin", "Khodor", "Chaoshi", "Lihe", "Xinyu", "Qixian", "Songyan", "Cao","Paul", "Omar", "Adrien", "Ivan", "Louis", "Julien", "Marion", "Claire", "Charles", "Julie","Alexandre", "Diane", "Capucine", "Victor", "Antoine")
        val random_lastname = List("Boulanger", "Charpentier", "Dorffer", "Stepanian", "Allouache", "Hammoud", "Petit", "Broussole", "Martin", "Bernard", "Robert", "Richard","Wang", "Li", "Zhang'", "Liu", "Chen", "Yang", "Zhao", "Huang", "Wu", "Zhou", "Yuan")
        val random_address = List("Arbres", "Poissons", "Champs", "Martyrs", "Hongrois", "Concombres", "Italiens", "Espagnols", "Français", "Marcassins")

        val name = Identity(random_name(randomizer.nextInt(random_name.length)),
          random_lastname(randomizer.nextInt(random_lastname.length)),
          randomizer.nextInt(200).toString() + " Rue des " + random_address(randomizer.nextInt(random_address.length)),
          randomizer.nextInt(100))

        val words = List("Bonjour", "Comment", "Bien", "Aidez-nous!", "Paris", "Vive notre grande patrie", "La voix du peuple sera entendue", "A l'aide !", "La nouvelle révolution",
          "J'ai peur", "Attention!", "Pourquoi?", "Je t'aime", "Soyons unis", "Ensemble", "Seul", "Dieu est grand", "N'oubliez pas les mots anciens",
          "Les damnés de la terre", "La fin des élites", "Résistons à la Grande Invasion!", "A mort les infidèles!",
          "Ils ont corrompu notre nation", "Ou est notre sauveteur de l'Ouest?",
          "Les peacewatchers arrivent", "Gloire au nouvel ordre mondial")

        def list_words(n_sentences : Int, word_list : List[String] = List()) : List[String] = {
          (n_sentences, word_list) match {
            case (n_sentences, word_list) if n_sentences > 0 =>
             list_words(n_sentences-1,word_list ++ words(randomizer.nextInt(words.length)).split(" "))
            case (1, word_list) => word_list ++ words(randomizer.nextInt(words.length)).split(" ")
            case _ => word_list
          }
        }

        val report = Drone(randomUUID().toString(), random_date(from, to), 47 + randomizer.nextFloat(),
                            2 + randomizer.nextFloat(), list_words(3 + randomizer.nextInt(10)), name)

        println("Report ", report)
        println()

        implicit val formats = DefaultFormats
        val jsonString = write(report)

        //println("JSONSTRING")
        //println(jsonString)
        
        producer.send(new ProducerRecord(topic, jsonString, name.peacescore))
        producer.send(new ProducerRecord(topic_file, jsonString, name.peacescore))

      }
    }

    timer.schedule(task, milliseconds_per_task, milliseconds_per_task)
    concurrent.TimeUnit.MILLISECONDS.sleep(n_tasks*milliseconds_per_task)
    timer.cancel()
    timer.purge()
    print("End Time (producer): " + System.currentTimeMillis())
    producer.close()
  }
}

