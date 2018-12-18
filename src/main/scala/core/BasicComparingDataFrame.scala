package core

import org.apache.spark.sql.functions._
import info.{DataFrameInfo, PrimaryKeysInfo}

object BasicComparingDataFrame extends DataFrameComparator {

  override def comparing(df1Info: DataFrameInfo, df2Info: DataFrameInfo, pkInfo: PrimaryKeysInfo): ComparingResult = {

    // DF名を取得
    val df1Name = df1Info.name
    val df2Name = df2Info.name

    // DFを取得
    val df1 = df1Info.make()
    val df2 = df2Info.make()

    // DFのカラム
    val df1Cols = df1.columns
    val df2Cols = df2.columns

    // 共通のカラムを取得
    val commonCols = df1Cols.filter(df2Cols.contains(_))
    // DF1のみのカラムを取得
    val onlyDf1Cols = df1Cols.filterNot(commonCols.contains(_))
    // DF2のみのカラムを取得
    val onlyDf2Cols = df2Cols.filterNot(commonCols.contains(_))

    // 共通部分
    val commonDf1 = df1.select(commonCols.map(col(_)):_*)
    val commonDf2 = df2.select(commonCols.map(col(_)):_*)

    val joinCondition = commonDf1.col("[PK]NAME") === df2.col("[PK]NAME")
    val joinedDf = commonDf1.join(commonDf2, joinCondition, "inner")
    ComparingResult.apply(joinedDf)(df1Name, df2Name)

  }

  def comparing(df1Info: DataFrameInfo, df2Info: DataFrameInfo): ComparingResult = {

    // DF名を取得
    val df1Name = df1Info.name
    val df2Name = df2Info.name

    // 主キーを取得
    val df1Pks = df1Info.getPrimaryKeys
    val df2Pks = df1Info.getPrimaryKeys

    // PK情報を作成
    val pkInfo = PrimaryKeysInfo(df1Name, df2Name)
    pkInfo.setComparingKeyPairs(df1Pks.filter(pk => df2Pks.contains(pk)).zip(df2Pks))

    comparing(df1Info, df2Info, pkInfo)

  }

}
