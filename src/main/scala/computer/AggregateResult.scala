package computer

import org.apache.spark.sql.DataFrame

case class AggregateResult(df: DataFrame) extends Result[DataFrame, AggregateLogic](df) {

  override protected val logic: AggregateLogic = new AggregateLogic

}

class AggregateLogic extends Logic[DataFrame] {

  override protected def compute(t: DataFrame): Unit = {



  }

}