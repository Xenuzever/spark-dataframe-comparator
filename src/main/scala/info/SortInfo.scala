package info

class SortInfo(name1: String, name2: String) extends ComparingInfo[(String, String, Boolean)](name1, name2) {

  val ASC = true

  val DESC = !ASC

//  override def append(info: (String, String, Boolean)) = {
//    super.append((Array.apply(name1, info._1).mkString("."),
//      Array.apply(name2, info._2).mkString(".")))
//  }

}