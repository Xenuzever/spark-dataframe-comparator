package info


case class ComparingInfo[T](df1Name: String, df2Name: String) {

  private var infoList = List.empty[T]

  def append(info: T) = {
    infoList = infoList :+ info
    this
  }

  def get = infoList

}