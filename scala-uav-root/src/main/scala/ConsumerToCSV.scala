import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.FileRecords.open
import net.liftweb.json.{DefaultFormats, parse}

import java.util.Properties
import java.util.Collections
import java.time.Duration
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import com.github.tototoshi.csv._

import java.io.{BufferedWriter, File, FileWriter}

import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import scala.util.Random
import java.util.{Properties, Timer, TimerTask, concurrent}

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit


object ConsumerToCSV extends App {
  def run(): Unit = {

    val topic = "testtopicfile"
    val fileName = "testdd.csv"
    val timeout = 20
    val columnNames = List("id", "drone_time", "lat_location", "long_location", "words", "surround", "peacescore")

    val props_con = new Properties()

    props_con.put("bootstrap.servers", "localhost:9092")
    props_con.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props_con.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
    props_con.put("auto.offset.reset", "earliest")
    props_con.put("group.id", "MessagesListDrone")

    val consumer = new KafkaConsumer[String, Int](props_con)
    consumer.subscribe(Collections.singletonList(topic))
    //consumer.seekToBeginning(consumer.assignment())
    val writer = new BufferedWriter(new FileWriter(fileName, true))

    def handler(time: Int, timeout: Int): Unit = {
      if (time > 0) {
        val records = consumer.poll(timeout).asScala
        records.foreach(record => {
          if (io.Source.fromFile(fileName).getLines.size == 0) {
            writer.write(columnNames.mkString(","))
          }
          writer.newLine()
          
          implicit val formats = DefaultFormats
          val jvalue = parse(record.key())
          
          val rec = jvalue.extract[DroneReport.Drone]

          val formReport = rec.id + "," + rec.drone_time + "," + rec.lat_location + "," + rec.long_location + "," + rec.words.mkString(";") + "," + rec.surround.lastname + "," + rec.surround.firstname + "," + rec.surround.address + "," + rec.surround.peacescore //rec.surround.map(x => x.lastname + "," + x.firstname + "," + x.address + "," + x.peacescore)
          writer.write(formReport)
        })
        concurrent.TimeUnit.MILLISECONDS.sleep(timeout)
        println()
        println("Iteration " + time)
        println()
        handler(time-timeout, timeout)
      }
    }

    handler(20000, 1000);
    writer.close()

    print("End Time (consumer): " + System.currentTimeMillis())
    consumer.close()
  }
}

