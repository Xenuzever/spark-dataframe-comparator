package result

import compare.ComparingColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

case class MatchingResult(rowCnt: Long, matchedItemCnt: Long, unMatchedItemCnt: Long) extends ResultData {
  /** EMPTY */
}

object MatchingResult extends Result[DataFrame, MatchingResult] {

  override def analyze(t: DataFrame): MatchingResult = {
    val columns = t.columns
    val rowCnt = t.count()
    val selectExpr = columns.filter(_.startsWith(ComparingColumns.COMPARING)).map(col(_))
    val comparingDF = t.select(selectExpr:_*)
    val matchedCnt = comparingDF.collect().map(f => f.toSeq.count(_.equals("○"))).sum
    val unMatchedCnt = (rowCnt * comparingDF.columns.length) - matchedCnt
    MatchingResult.apply(rowCnt, matchedCnt, unMatchedCnt)
  }

}
