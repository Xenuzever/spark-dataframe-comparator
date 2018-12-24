package editor

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

trait DataFrameEditor[T] {

  protected def logic(df: DataFrame, t: T): DataFrame

  final def edit(df: DataFrame, t: T): DataFrame = {
    logic(df, t)
  }

}
