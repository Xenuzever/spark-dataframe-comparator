/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package compare

import org.apache.spark.sql.DataFrame
import result.MatchingResult

final class MatchingDataFrames extends ComparingDataFrames[MatchingResult](JoiningDataFrames.INNER) {

  override protected def createResult(df: DataFrame): MatchingResult = {
    MatchingResult.apply(df).analyzed
  }

}
