package core

import info.DataFrameInformation

trait DataFrameComparator {

  final val INNER_JOIN = "inner"

  final val LEFT_OUTER_JOIN = "left_outer"

  final val RIGHT_OUTER_JOIN = "right_outer"

  final val CROSS_JOIN = "cross"

  final val FULL_OUTER_JOIN = "full_outer"

  def comparing(df1Info: DataFrameInformation, df2Info: DataFrameInformation, join: String = INNER_JOIN): ComparingResult

}
