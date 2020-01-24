package test.miquifant

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util._

object GeneralProducer {

  def main(args: Array[String]): Unit = {

    val props:Properties = new Properties()
    props.put("bootstrap.servers", "quickstart.cloudera:9092")
    props.put("key.serializer",    "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",  "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks",              "1")

    val producer = new KafkaProducer[String, String](props)
    val topic = "miprima"

    try {
      Range(0, 10).foreach { i =>
        val message = new ProducerRecord[String, String](
          topic,
          (i % 2).toString,
          s"message $i content"
        )
        val metadata = producer.send(message).get()
        printf(s"Enviando mensaje (key=%s, value=%s), meta (partition=%d, offset=%d, topic=%s)\n",
          message.key(),
          message.value(),
          metadata.partition(),
          metadata.offset(),
          metadata.topic()
        )
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      producer.close()
    }
  }
}
