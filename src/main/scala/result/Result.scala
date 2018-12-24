package result

import org.apache.spark.sql.DataFrame

abstract class Result {

  def analyze(t: DataFrame): Unit

}
