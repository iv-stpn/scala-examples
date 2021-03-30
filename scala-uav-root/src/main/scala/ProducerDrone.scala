import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.concurrent.TimeUnit
import scala.util.Random
import java.util.{Properties, Timer, TimerTask, concurrent}

object ProducerDrone extends App {
  def test(identity:DroneReport.Identity,drone: String): Unit = {
    val topic = "testtopic"


    val rand = scala.util.Random
    val score = rand.nextInt(100)
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")

    val time = System.currentTimeMillis()
    val producer = new KafkaProducer[String, Int](props)
    val timer = new Timer()
    val task = new TimerTask {
      override def run(): Unit = producer.send(new ProducerRecord(topic, drone, rand.nextInt(100)))
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

