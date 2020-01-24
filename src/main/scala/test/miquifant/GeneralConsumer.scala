package test.miquifant

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

import java.time.Duration
import java.util.Properties

object GeneralConsumer {
  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties()
    props.put("group.id",           "test")
    props.put("bootstrap.servers",  "quickstart.cloudera:9092")
    props.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)
    val topics = List("miprima")

    try {
      consumer.subscribe(topics.asJava)
      while (true) {
        consumer.poll(Duration.ofMillis(10)).asScala.foreach(println)
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      consumer.close()
    }
  }
}
