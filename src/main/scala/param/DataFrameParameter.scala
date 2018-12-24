/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package param

import java.util.regex.Pattern

import org.apache.spark.sql.DataFrame

class DataFrameParameter(name: String,
                         df: DataFrame,
                         primaryKeyParameter: PrimaryKeyParameter = new PrimaryKeyParameterBuilder().build,
                         litValueParameter: LitValueParameter = new LitValueParameterBuilder().build,
                         renameParameter: RenameParameter = new RenameParameterBuilder().build,
                         prefixParameter: ColumnPrefixParameter = new ColumnPrefixParameterBuilder().build,
                         suffixParameter: ColumnSuffixParameter = new ColumnSuffixParameterBuilder()build)
  extends Parameter[DataFrame, DataFrame] {

  val paramName = name

  val columns = dataFrame.columns

  val primaryKeys = {
    val regex = s"^($PrimaryKeyParameter.PK)(.+)" + "$"
    columns
      .map(Pattern.compile(regex).matcher(_))
      .filter(_.find())
      .map(_.group(2))
  }

  val attributes = columns.filterNot(primaryKeys.contains(_))

  lazy val dataFrame = convert(df)

  lazy val columnMap: Map[String, String] = {
    df.columns
      .map(col => (col, col))
      .map(x => (renameParameter.convert(x._1), x._2))
      .map(x => (primaryKeyParameter.convert(x), x._2))
      .map(x => (prefixParameter.convert(x), x._2))
      .map(x => (suffixParameter.convert(x), x._2))
      .toMap[String, String]
  }

  override def convert(t: DataFrame): DataFrame = {
    litValueParameter.convert(df).transform(x => {
      var tmpDf = x
      columnMap.foreach(f => {
        tmpDf = tmpDf.withColumnRenamed(f._2, f._1)
      })
      tmpDf
    })
  }

}
