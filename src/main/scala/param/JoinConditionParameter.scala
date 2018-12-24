package param

import org.apache.spark.sql.Column

case class JoinConditionParameter(join: String)
  extends Parameter[(DataFrameParameter, DataFrameParameter), Column] {

  override def convert(t: (DataFrameParameter, DataFrameParameter)): Column = {
    val df1PK = t._1.primaryKeys
    val df2PK = t._2.primaryKeys
    val commonPK = df1PK.filter(df2PK.contains(_))
    commonPK
      .map(x => (t._1.dataFrame.col(x), t._2.dataFrame.col(x)))
      .
  }

}

object JoinConditionParameter {

  val INNER_JOIN = "inner"

  val LEFT_OUTER_JOIN = "left_outer"

  val RIGHT_OUTER_JOIN = "right_outer"

  val FULL_OUTER_JOIN = "full_outer"

  val CROSS_JOIN = "cross"

}
