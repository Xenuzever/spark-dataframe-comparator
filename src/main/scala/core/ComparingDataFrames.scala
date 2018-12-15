package core

import org.apache.spark.sql.DataFrame

case class ComparingDataFrames(df1Name: String = "DF1", df2Name: String = "DF2") extends DataFrameComparator {

  override def comparing(df1: DataFrame, df2: DataFrame): ComparingResult = {
    null
  }

}
