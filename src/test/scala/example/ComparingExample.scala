package example

import comparing.ComparingDataFrames
import information.DataFrameInformation
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
    val df1Cols = Array.apply("ID", "NAME", "AGE", "HEIGHT", "WEIGHT", "HOBBY")
    val df1Data = Array.apply(
      Array.apply("1", "John", "23", "173", "63", "GAME"),
      Array.apply("2", "Michel", "34", "168", "70", null),
      Array.apply("3", "Emma", "18", "153", "52", "Cooking"),
      Array.apply("4", "Mason", "45", "159", "56", "Soccer"),
      Array.apply("5", "Jacob", "28", "183", null, "Basket ball")
    )

    // DF2's data.
    val df2Cols = Array.apply("ID", "NAME", "AGE", "HEIGHT", "WEIGHT", "LIKES")
    val df2Data = Array.apply(
      Array.apply("1", "John", "23", "173", "63", "GAME"),
      Array.apply("2", "Michel", "34", "168", "70", "Drinking"),
      Array.apply("3", "Emma", "18", "153", "52", "Programming"),
      Array.apply("4", "Mason", "45", "159", "56", "Soccer"),
      Array.apply("5", "Jacob", "63", "184", null, "Guitar")
    )

    // Define Info's arguments.
    val df1Name = "DF1"
    val df1 = createInstantDataFrame(df1Cols, df1Data)
    val df2Name = "DF2"
    val df2 = createInstantDataFrame(df2Cols, df2Data)

    // Create DataFrame Info.
    val df1Info = DataFrameInformation.apply(df1Name, df1)
      .appendPrimaryKey("ID")
      .appendLitValue("NEW_HEIGHT", 160)

    val df2Info = DataFrameInformation.apply(df2Name, df2)
      .appendPrimaryKey("ID")
      .copyColumn("HEIGHT", "NEW_HEIGHT")

    // Compare and get result.
    val result = ComparingDataFrames.comparing(df1Info, df2Info)

    // Show result.
    result.df.show()
    println(result.name)

  }

  def createInstantDataFrame(columns: Array[String], data: Array[Array[String]]): DataFrame = {
    val fields = columns.map(f => StructField(f, StringType, true))
    val schema = StructType.apply(fields)
    val rows = data.map(r => Row(r:_*))
    val rowRDD = spark.sparkContext.parallelize(rows)
    spark.createDataFrame(rowRDD, schema)
  }

}
