import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.Properties
import java.util.Collections
import java.time.Duration
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import scala.util.Random
import java.util.{Properties, Timer, TimerTask, concurrent}

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import org.apache.log4j.Logger
import org.apache.log4j.Level

object ConsumerDrone extends App {
  def run(total_milliseconds : Int, milliseconds_per_task : Int): Unit = { 
    /*Logger.getRootLogger().setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)*/

    val topic = "testtopic"
    val props_con = new Properties()

    props_con.put("bootstrap.servers", "localhost:9092")
    props_con.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props_con.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
    props_con.put("auto.offset.reset", "earliest")
    props_con.put("group.id", "MessagesListDrone")


    val consumer = new KafkaConsumer[String, Int](props_con)
    consumer.subscribe(Collections.singletonList(topic))
    //consumer.seekToBeginning(consumer.assignment())

    def handler(time: Int, timeout: Int): Unit = {
      if (time > 0) {
        val records = consumer.poll(timeout).asScala

        records.foreach(record => {
          if(record.value() > 50) {
            println()
            println("Problem" + record.key() + " " + record.value())
            println()
          }
          else {
            println()
            println("No problem")
            println()
          }
        })
        concurrent.TimeUnit.MILLISECONDS.sleep(timeout)
        println()
        println("Iteration " + time)
        println()
        handler(time-timeout, timeout)
      }
    }

    handler(total_milliseconds+(milliseconds_per_task*1.5).toInt, (milliseconds_per_task*1.2).toInt);
    print("End Time (consumer): " + System.currentTimeMillis())
    consumer.close()
  }
}
