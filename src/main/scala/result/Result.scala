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

abstract class Result[T <: Result[T]](t: DataFrame) {

  val dataFrame = t

  final lazy val analyzed = analyze

  protected def analyze: T

}
