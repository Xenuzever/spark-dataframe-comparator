package info

import creator.Creator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

case class PKInformation(df1Info: DataFrameInformation, df2Info: DataFrameInformation)
  extends ComparingInformation[DataFrameInformation, String](df1Info, df2Info) with Creator[PKInformation, Column] {

  val df1pkArray = df1Info.getPrimaryKeySet.toArray
  val df2pkArray = df2Info.getPrimaryKeySet.toArray

  if (df1pkArray.size > 0 && df1pkArray.size > 0) {
    setComparingKeyPairs(df1pkArray.filter(pk => df2pkArray.contains(pk)).zip(df2pkArray))
  }

  override def appendPair[R >: ComparingInformation[DataFrameInformation, String]](u1: String, u2: String): R = {
    super.appendPair(u1, u2)
    this
  }

  def setComparingKeyPairs(keys: Array[(String, String)]): Unit = {
    clear
    keys.foreach(x => appendPair(x._1, x._2))
  }

  override def create: Column = {
    val df1 = df1Info.createdDf
    val df2 = df2Info.createdDf
    getPairs.map(x => df1.col(x._1) === df2.col(x._2))
      .fold(lit(0) === lit(0))((x1, x2) => x1.and(x2))
  }

}
