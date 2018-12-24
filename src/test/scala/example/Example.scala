package example

import compare.ComparingDataFrames
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import param.{DataFrameParameter, PrimaryKeyParameter}
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
    val primaryKeyParam = PrimaryKeyParameter.Builder
      .append("ID")
      .append("NAME").build
    //

    // Create DataFrame Info.
    val df1Param = DataFrameParameter.apply(df1Name, df1, primaryKeyParam)
    val df2Param = DataFrameParameter.apply(df2Name, df2, primaryKeyParam)

    // Compare and get result.
    val result = ComparingDataFrames.comparing(df1Param, df2Param)
    MatchingResult.analyze(result)
    // Show result.
    //result.df.show()

    // Show result name.
    //println(result.name)

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
      Array.apply("4", "Mason", "45", "159", "56", "Soccer"),
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
