package test.miquifant.serializer

import java.nio.ByteBuffer
import java.util

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

class UserSerializer extends Serializer[User] {

  private val encoding = "UTF8"

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(topic: String, user: User): Array[Byte] = {
    try {
      val serializedName = user.getName.getBytes(encoding)
      val nameSize = serializedName.length
      val buf = ByteBuffer.allocate(nameSize + 50) // TODO that 50...
      buf.putInt(user.getUserId)
      buf.putInt(nameSize)
      buf.put(serializedName)
      buf.array()
    }
    catch {
      case e: Exception => throw new SerializationException(s"Unable to serialize User $user")
    }
  }

  override def close(): Unit = {}
}
