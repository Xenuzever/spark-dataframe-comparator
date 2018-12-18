package core

import info.{DataFrameInfo, PrimaryKeysInfo}

trait DataFrameComparator {

  def comparing(df1Info: DataFrameInfo, df2Info: DataFrameInfo, pkInfo: PrimaryKeysInfo): ComparingResult

}
