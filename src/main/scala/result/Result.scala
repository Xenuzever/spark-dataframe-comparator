/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package result

import org.apache.spark.sql.DataFrame

abstract class Result[A <: AnalyzedData, T <: Result[A, T]](t: DataFrame) {

  val dataFrame = t

  final lazy val analyzed: A = analyze

  protected def analyze: A

}

abstract class AnalyzedData
