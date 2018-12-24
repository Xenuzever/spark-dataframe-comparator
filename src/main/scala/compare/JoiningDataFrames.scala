package compare

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

case class JoiningDataFrames(df1: DataFrame, df2: DataFrame) {

  def joined(join: String, primaryKeys: Array[String], df1ColMap: Map[String, String], df2ColMap: Map[String, String])
  : DataFrame = {
    val joinCondition = primaryKeys
      .map(x => (df1.col(df1ColMap.get(x).get), df2.col(df2ColMap.get(x).get)))
      .map(x => x._1 === x._2)
      .fold(lit(0) === lit(0))((x1, x2) => x1.and(x2))
    df1.join(df2, joinCondition, join)
  }

}

object JoiningDataFrames {

  val INNER = "inner"

  val LEFT_OUTER = "left_outer"

  val RIGHT_OUTER = "right_outer"

  val FULL_OUTER = "full_outer"

  val CROSS = "cross"

}
