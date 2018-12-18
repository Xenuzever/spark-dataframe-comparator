package core

import org.apache.spark.sql.DataFrame

case class ComparingResult(resultDf: DataFrame)(implicit df1Name: String, df2Name: String) {

  def getDataFrame = resultDf

  def getDf1Name = df1Name

  def getDf2Name = df2Name

}
