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

abstract class Result(t: DataFrame) {

  val dataFrame = t

  analyze

  protected def analyze: Result

}
