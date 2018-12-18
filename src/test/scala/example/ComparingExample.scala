package example

import core.BasicComparingDataFrame
import info.DataFrameInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import scala.reflect.io.Path


object ComparingExample {

  // Spark entry point.
  val spark = SparkSession
    .builder()
    .appName("ComparingExample")
    .master("local[*]")
    .getOrCreate()

  /**
    * Main method.
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // DF1's data.
    val df1Cols = Array.apply("NAME", "AGE", "HEIGHT", "WEIGHT", "HOBBY")
    val df1Data = Array.apply(
      Array.apply("John", "23", "173", "63", "GAME"),
      Array.apply("Michel", "34", "168", "70", null),
      Array.apply("Emma", "18", "153", "52", "Cooking"),
      Array.apply("Mason", "45", "159", "56", "Soccer"),
      Array.apply("Jacob", "28", "183", "72", "Basket ball")
    )

    // DF2's data.
    val df2Cols = Array.apply("NAME", "AGE", "HEIGHT", "WEIGHT", "LIKES")
    val df2Data = Array.apply(
      Array.apply("Johnathan", "23", "173", "63", "GAME"),
      Array.apply("Michael", "34", "168", "70", "Drinking"),
      Array.apply("Emma", "18", "153", "52", "Programming"),
      Array.apply("Mason", "45", "159", "56", "Soccer"),
      Array.apply("Jacob", "63", "184", "72", "Guitar")
    )

    // Define Info's arguments.
    val df1Name = "DF1"
    val df1 = createInstantDataFrame(df1Cols, df1Data)
    val df2Name = "DF2"
    val df2 = createInstantDataFrame(df2Cols, df2Data)

    // Create DataFrame Info.
    val df1Info = DataFrameInfo.apply(df1Name, df1)
      .appendPrimaryKey("NAME")

    val df2Info = DataFrameInfo.apply(df2Name, df2)
      .appendPrimaryKey("NAME")

    // Compare and get result.
    val result = BasicComparingDataFrame.comparing(df1Info, df2Info)

    // Show result.
    result.resultDf.show()

  }

  def createInstantDataFrame(columns: Array[String], data: Array[Array[String]]): DataFrame = {
    val fields = columns.map(f => StructField(f, StringType, true))
    val schema = StructType.apply(fields)
    val rows = data.map(r => Row(r:_*))
    val rowRDD = spark.sparkContext.parallelize(rows)
    spark.createDataFrame(rowRDD, schema)
  }

}
