package example

import info.{ColumnInfo, PrimaryKeyInfo, Replace}

object FileComparingExample {

  def main(args: Array[String]): Unit = {

    val df1Name = "DF1"
    val df2Name = "DF2"

    val pkInfo = new PrimaryKeyInfo(df1Name, df2Name)
    pkInfo.append(("A", "A"))
    pkInfo.append(("B", "B"))

    val column = new ColumnInfo


    val replace = Replace.apply("C", "NULL")

    val pk = Array.apply("A", "B")
    val sort = Array.apply(("DF1", "B"), ("DF2", "B"), ("DF1", "C"), ("DF2", C"))

  }

}
