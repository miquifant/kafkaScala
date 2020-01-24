package test.miquifant.serializer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util._

object SerializingProducer {
  def main(args: Array[String]): Unit = {
    val props:Properties = new Properties()
    props.put("bootstrap.servers", "quickstart.cloudera:9092")
    props.put("key.serializer",    "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",  classOf[UserSerializer].getCanonicalName)
    props.put("acks",              "1")

    val producer = new KafkaProducer[String, User](props)
    val topic = "miprima_part"

    try {
      Range(0, 10).foreach { i =>
        val user = new User(i, s"Usuario-$i")
        val message = new ProducerRecord[String, User](
          topic,
          (i % 2).toString,
          user
        )
        val metadata = producer.send(message).get()
        printf(
          s"Enviando mensaje (key=%s, value=%s), meta (partition=%d, offset=%d, topic=%s\n",
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
