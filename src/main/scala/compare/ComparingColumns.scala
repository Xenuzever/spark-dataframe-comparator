/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package compare

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object ComparingColumns extends Comparator[Column, Column] {

  final val COMPARING = "[COMPARE]"

  private val comparingColumns = udf((col1: String, col2: String) => {
    comparingLogic(col1, col2)
  })

  override def comparing(col1: Column, col2: Column): Column = {
    comparingColumns(col1, col2)
  }

  def createComparingColumnName(colName1: String, colName2: String): String = {
    s"$COMPARING$colName1:$colName2"
  }

  protected def comparingLogic(col1: String, col2: String): String = {
    if (col1 == col2) "○"
    else "×"
  }

}
