package test.miquifant.serializer

import java.nio.ByteBuffer
import java.util

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

class UserDeserializer extends Deserializer[User] {

  private val encoding = "UTF8"

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): User = {
    try {
      val buf = ByteBuffer.wrap(data)
      val userId = buf.getInt()
      val nameSize = buf.getInt()
      val serializedName = new Array[Byte](nameSize)
      buf.get(serializedName)
      val userName = new String(serializedName, encoding)

      new User(userId, userName)
    }
    catch {
      case e: Exception => throw new SerializationException(s"Unable to deserialize User")
    }
  }

  override def close(): Unit = {}
}
