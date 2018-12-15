package core

import org.apache.spark.sql.DataFrame

trait DataFrameComparator {

  abstract def comparing(df1: DataFrame, df2: DataFrame): ComparingResult

}
