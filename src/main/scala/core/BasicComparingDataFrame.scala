package core

import org.apache.spark.sql.functions._
import info.{DataFrameInformation, PKInformation}

object BasicComparingDataFrame extends DataFrameComparator {

  override def comparing(df1Info: DataFrameInformation, df2Info: DataFrameInformation, join: String): ComparingResult = {

    // Get DataFrame names.
    val df1Name = df1Info.name
    val df2Name = df2Info.name

    // Get DataFrame.
    val df1 = df1Info.createdDf
    val df2 = df2Info.createdDf

    // DFのカラム
    val df1Cols = df1.columns
    val df2Cols = df2.columns

    // Get common columns.
    val commonCols = df1Cols.filter(df2Cols.contains(_))
    // Get only df1 columns.
    val onlyDf1Cols = df1Cols.filterNot(commonCols.contains(_))
    // Get only df2 columns.
    val onlyDf2Cols = df2Cols.filterNot(commonCols.contains(_))

    // Create common DataFrames.
    val commonDf1 = df1.select(commonCols.map(df1.col(_)):_*)
    val commonDf2 = df2.select(commonCols.map(df2.col(_)):_*)

    // Creat select phrase.
    val zippedCommonCols = commonCols.zip(commonCols)
    val select = Array.concat(
      zippedCommonCols.filter(_._1.contains(DataFrameInformation.PK)),
      zippedCommonCols.filterNot(_._1.contains(DataFrameInformation.PK))
    )
      .map(x => Array.apply(x._1, x._2).mkString(","))
      .mkString(",")
      .split(",")

    // Create PKInformation.
    val pkInfo = new PKInformation(df1Info, df2Info)

    // DF1 join to DF2.
    val joinCondition = pkInfo.create
    val joinedDf = commonDf1.join(commonDf2, joinCondition, join)
        .transform(x => {
          x
        })
        //.select(select.map(col(_)):_*)


    ComparingResult.apply(joinedDf)(df1Name, df2Name)


  }

}
