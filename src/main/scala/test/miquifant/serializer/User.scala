package test.miquifant.serializer

class User (var userId: Int, var name: String) {
  def getUserId: Int = userId
  def getName: String = name

  override def toString: String = s"Referencia: $userId, name: '$name'"
}
