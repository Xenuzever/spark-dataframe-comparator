/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package example

import compare.{ComparingDataFrames, JoiningDataFrames, MatchingDataFrames}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import param._
import result.MatchingResult

object Example {

  // Spark entry point.
  val spark = SparkSession
    .builder()
    .appName("Example")
    .master("local[*]")
    .getOrCreate()

  /**
    * Main method.
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // DataFrame1
    val df1Name = "DF1"
    val df1 = createTestData1

    // DataFrame2
    val df2Name = "DF2"
    val df2 = createTestData2

    // PK param
    val primaryKeyParam = new PrimaryKeyParameterBuilder()
      .append("ID")
      .append("NAME")
      .build

    // LIT param
    val litParam = new LitValueParameterBuilder()
      .append("AGE", "30")
      .build

    // PREFIX param
    val prefixParam = new ColumnPrefixParameterBuilder()
      .append("AGE", "[LIT]")
      .build

    // Create DataFrame Info.
    val df1Param = new DataFrameParameter(df1Name, df1, primaryKeyParam, litParam, prefixParameter = prefixParam)
    val df2Param = new DataFrameParameter(df2Name, df2, primaryKeyParam, prefixParameter = prefixParam)

    // Compare and get result.
    val mdf = new MatchingDataFrames()
    val result = mdf.comparing(df1Param, df2Param)

    // Show result.
    val resultDf = result.dataFrame
    resultDf.show()
//    val analyzed = result.analyzed
//    val matchedRowCnt = analyzed.rowCnt
//    val matchedItemCnt = analyzed.matchedItemCnt
//    val unMatchedItemCnt = analyzed.unMatchedItemCnt
//    println(s"MATCHED ROWS: $matchedRowCnt, MATCHED ITEMS: $matchedItemCnt, UNMATCHED ITEMS: $unMatchedItemCnt")

  }

  def createTestData1 = {
    val df1Cols = Array.apply("ID", "NAME", "AGE", "HEIGHT", "WEIGHT", "HOBBY")
    val df1Data = Array.apply(
      Array.apply("1", "John", "23", "173", "63", "GAME"),
      Array.apply("2", "Michel", "34", "168", "70", null),
      Array.apply("3", "Emma", "18", "153", "52", "Cooking"),
      Array.apply("4", "Mason", "45", "159", "56", "Soccer"),
      Array.apply("5", "Jacob", "28", "183", null, "Basket ball")
    )
    createInstantDataFrame(df1Cols, df1Data)
  }

  def createTestData2 = {
    val df2Cols = Array.apply("ID", "NAME", "AGE", "HEIGHT", "WEIGHT", "LIKES")
    val df2Data = Array.apply(
      Array.apply("1", "John", "23", "173", "63", "GAME"),
      Array.apply("2", "Michel", "34", "168", "70", "Drinking"),
      Array.apply("3", "Emma", "18", "153", "52", "Programming"),
      Array.apply("4", "Masons", "45", "159", "56", "Soccer"),
      Array.apply("5", "Jacob", "63", "184", null, "Guitar")
    )
    createInstantDataFrame(df2Cols, df2Data)
  }

  def createInstantDataFrame(columns: Array[String], data: Array[Array[String]]): DataFrame = {
    val fields = columns.map(f => StructField(f, StringType, true))
    val schema = StructType.apply(fields)
    val rows = data.map(r => Row(r:_*))
    val rowRDD = spark.sparkContext.parallelize(rows)
    spark.createDataFrame(rowRDD, schema)
  }

}
