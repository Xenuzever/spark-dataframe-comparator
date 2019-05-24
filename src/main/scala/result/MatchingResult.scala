/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package result

import compare.ComparingColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

case class MatchingResult(df: DataFrame) extends Result[AnalyzedDataOfMatchingResult, MatchingResult](df) {

  override protected def analyze = AnalyzedDataOfMatchingResult.apply(df)

}

case class AnalyzedDataOfMatchingResult(df: DataFrame) extends AnalyzedData {

  private val selectExpr = df.columns.filter(_.startsWith(ComparingColumns.COMPARING)).map(col(_))
  private val comparingDF = df.select(selectExpr:_*)
  val rowCnt = df.count()
  val matchedItemCnt = comparingDF.rdd.map(f => f.toSeq.count(_.equals("○"))).sum.toLong
  val unMatchedItemCnt = (rowCnt * comparingDF.columns.length) - matchedItemCnt

}