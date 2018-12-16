package core

import info.{PrimaryKeyInfo, SortInfo}
import org.apache.spark.sql.DataFrame

abstract case class ComparingDataFrames[T <: ComparingResult]
(comparingKeys: Array[PrimaryKeyInfo] = Array.empty[PrimaryKeyInfo],
 sorts: Array[SortInfo] = Array.empty[SortInfo],
 replaces: Array[ReplaceInfo] = Array.empty[ReplaceInfo]) extends DataFrameComparator[T] {

  override protected def comparing(df1: (String, DataFrame), df2: (String, DataFrame)) = {
    val df1Name = df1._1; val df1Data = df1._2
    val df2Name = df2._1; val df2Data = df2._2
    val resultDf = df1Data.join(df2Data, "full_outer")

    execute(resultDf)
  }

  protected abstract def execute(resultDf: DataFrame): T

  def create


}
