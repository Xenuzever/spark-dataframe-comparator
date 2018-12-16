package core

import org.apache.spark.sql.DataFrame

trait DataFrameComparator[T] {

  protected abstract def comparing(df1: (String, DataFrame), df2: (String, DataFrame)): T

}
