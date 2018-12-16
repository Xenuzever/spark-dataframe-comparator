package info

class PrimaryKeyInfo(name1: String, name2: String) extends ComparingInfo[(String, String)](name1, name2) {

  override def append(info: (String, String)) = {
    super.append((Array.apply(name1, info._1).mkString("."),
      Array.apply(name2, info._2).mkString(".")))
  }

}
