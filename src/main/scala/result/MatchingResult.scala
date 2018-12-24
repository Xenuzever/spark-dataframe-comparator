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

case class MatchingResult(df: DataFrame) extends Result[MatchingResult](df) {

  private var rowCnt: Long = 0

  private var matchedItemCnt: Long = 0

  private var unMatchedItemCnt: Long = 0

  override protected def analyze = {
    val selectExpr = df.columns.filter(_.startsWith(ComparingColumns.COMPARING)).map(col(_))
    val comparingDF = df.select(selectExpr:_*)
    this.rowCnt = df.count()
    this.matchedItemCnt = comparingDF.collect().map(f => f.toSeq.count(_.equals("○"))).sum
    this.unMatchedItemCnt = (rowCnt * comparingDF.columns.length) - matchedItemCnt
    this
  }

  def getRowCnt = rowCnt

  def getMatchedItemCnt = matchedItemCnt

  def getUnMatchedItemCnt = unMatchedItemCnt

}