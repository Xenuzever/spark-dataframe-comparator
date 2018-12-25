/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package compare

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import param.{DataFrameParameter, PrimaryKeyParameter}
import result.Result

abstract class ComparingDataFrames[T <: Result[T]](join: String) extends Comparator[DataFrameParameter, T] {

  protected def createResult(df: DataFrame): T

  override def comparing(df1Param: DataFrameParameter, df2Param: DataFrameParameter): T = {

    val df1Name = df1Param.paramName
    val df2Name = df2Param.paramName

    val df1 = df1Param.dataFrame
    val df2 = df2Param.dataFrame

    val df1Cols = df1Param.columns
    val df2Cols = df2Param.columns

    val df1ColMap = df1Param.columns.map(x => (x, s"$df1Name-$x")).toMap
    val df2ColMap = df2Param.columns.map(x => (x, s"$df2Name-$x")).toMap

    val commonCols = df1Cols.filter(df2Cols.contains(_))
    val commonPK = commonCols.filter(_.contains(PrimaryKeyParameter.PK))
    val onlyDF1Cols = df1Cols.filterNot(commonCols.contains(_))
    val onlyDF2Cols = df2Cols.filterNot(commonCols.contains(_))

    val compareDF1 = df1.select(Array.concat(commonCols, onlyDF1Cols).map(df1.col):_*).transform(x => {
      var tmpDF = x
      df1Cols.foreach(c => {
        val newName = df1ColMap.get(c).get
        tmpDF = tmpDF
          .withColumnRenamed(c, newName)
      })
      tmpDF
    })

    val compareDF2 = df2.select(Array.concat(commonCols, onlyDF2Cols).map(df2.col):_*).transform(x => {
      var tmpDF = x
      df2Cols.foreach(c => {
        val newName = df2ColMap.get(c).get
        tmpDF = tmpDF
          .withColumnRenamed(c, newName)
      })
      tmpDF
    })

    val zippedPKCols = commonPK
      .map(x => (df1ColMap.get(x).get, df2ColMap.get(x).get))

    val zippedNotPKCols = commonCols.filterNot(commonPK.contains(_))
      .map(x => {
        val newName1 = df1ColMap.get(x).get
        val newName2 = df2ColMap.get(x).get
        (newName1, newName2, ComparingColumns.createComparingColumnName(newName1, newName2))
      })

    val selectColumns = Seq(
      Seq(zippedPKCols.map(x => Array.apply(x._1, x._2)).flatten).flatten,
      Seq(zippedNotPKCols.map(x => Array.apply(x._1, x._2, x._3)).flatten).flatten,
      Seq(onlyDF1Cols.map(df1ColMap.get(_).get).toSeq).flatten,
      Seq(onlyDF2Cols.map(df2ColMap.get(_).get).toSeq).flatten
    )
      .flatten
      .map(x => x.toString)
      .map(col)

    // DF1 join to DF2.
    val joinedDF = JoiningDataFrames
      .apply(compareDF1, compareDF2)
      .joined(join, commonPK, df1ColMap, df2ColMap)
      .transform(x => {
        var tmpDf = x
        zippedNotPKCols.foreach(x => {
          tmpDf = tmpDf.withColumn(x._3, ComparingColumns.comparing(col(x._1), col(x._2)))
        })
        tmpDf
      })
      .select(selectColumns:_*)

    // Result
    createResult(joinedDF).analyzed

  }

}
