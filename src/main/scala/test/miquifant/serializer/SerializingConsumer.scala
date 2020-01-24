package test.miquifant.serializer

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

import java.time.Duration
import java.util.Properties

object SerializingConsumer {
  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties()
    props.put("group.id",           "test")
    props.put("bootstrap.servers",  "quickstart.cloudera:9092")
    props.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", classOf[UserDeserializer].getCanonicalName)

    val consumer = new KafkaConsumer[String, User](props)
    val topics = List("miprima_part")

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
