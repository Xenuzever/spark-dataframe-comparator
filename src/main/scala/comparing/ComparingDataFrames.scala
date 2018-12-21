package comparing

import org.apache.spark.sql.functions._
import information.{DataFrameInformation, PKInformation}

object ComparingDataFrames extends Comparator[DataFrameInformation, DataFrameInformation] {

  final val INNER_JOIN = "inner"

  final val LEFT_OUTER_JOIN = "left_outer"

  final val RIGHT_OUTER_JOIN = "right_outer"

  final val CROSS_JOIN = "cross"

  final val FULL_OUTER_JOIN = "full_outer"

  override def comparing(df1Info: DataFrameInformation, df2Info: DataFrameInformation): DataFrameInformation = {
    comparing(df1Info, df2Info, INNER_JOIN)
  }

  def comparing(df1Info: DataFrameInformation, df2Info: DataFrameInformation, join: String)
  : DataFrameInformation = {

    // Get DataFrame names.
    val df1Name = df1Info.name
    val df2Name = df2Info.name

    // Get DataFrame.
    val df1 = df1Info.createdDf
    val df2 = df2Info.createdDf

    // Get DataFrame columns.
    val df1ColMap = df1Info.getColumnMap
    val df2ColMap = df2Info.getColumnMap
    val df1Cols = df1.columns
    val df2Cols = df2.columns

    // Create select columns for comparing DataFrame.
    val commonCols = df1ColMap.filter(x => df2ColMap.contains(x._1)).map(x => (x._2, df2ColMap.get(x._1).get))
    val commonDf1Cols = commonCols.map(_._1).toArray
    val commonDf2Cols = commonCols.map(_._2).toArray
    val onlyDf1Cols = df1Cols.filterNot(commonDf1Cols.contains(_))
    val onlyDf2Cols = df2Cols.filterNot(commonDf2Cols.contains(_))

    // Create comparing DataFrames.
    val compareDf1 = df1.select(Array.concat(commonDf1Cols, onlyDf1Cols).map(df1.col):_*)
    val compareDf2 = df2.select(Array.concat(commonDf2Cols, onlyDf2Cols).map(df2.col):_*)

    // Create select column for result DataFrame.
    val zippedPKCols = commonCols.filter(_._1.contains(DataFrameInformation.PK))
    val zippedNotPKCols = commonCols.filterNot(_._1.contains(DataFrameInformation.PK))
      .map(x => (x._1, x._2, ComparingColumns.createComparingColumnName(x._1, x._2)))
    val selectColumns = Seq(
      Seq(zippedPKCols.map(x => Array.apply(x._1, x._2)).flatten).flatten,
      Seq(zippedNotPKCols.map(x => Array.apply(x._1, x._2, x._3)).flatten).flatten,
      Seq(onlyDf1Cols.toSeq).flatten,
      Seq(onlyDf2Cols.toSeq).flatten
    )
      .flatten
      .map(x => x.toString)
      .map(col)

    // DF1 join to DF2.
    val pkInfo = PKInformation.apply(df1Info, df2Info)
    val joinCondition = pkInfo.create
    val joinedDf = compareDf1.join(compareDf2, joinCondition, join)
        .transform(x => {
          var tmpDf = x
          zippedNotPKCols.foreach(x => {
            tmpDf = tmpDf.withColumn(x._3, ComparingColumns.comparing(col(x._1), col(x._2)))
          })
          tmpDf
        })
        .select(selectColumns:_*)

    // Apply.
    val resultDfName = df1Name + "_" + join + "_joined_" + df2Name
    DataFrameInformation.apply(resultDfName, joinedDf)

  }

}
