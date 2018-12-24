package result

import org.apache.spark.sql.DataFrame

abstract class Result {

  private var df: DataFrame = null

  def analyze(t: DataFrame): Unit = {
    this.df = t
  }

  def getDF = df

}
