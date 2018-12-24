package param

import java.util.regex.Pattern

import editor.DataFrameEditor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

case class DataFrameParameter(name: String,
                              df: DataFrame,
                              primaryKeyParameter: PrimaryKeyParameter = PrimaryKeyParameter.Builder.build,
                              litValueParameter: LitValueParameter = LitValueParameter.Builder.build,
                              renameParameter: RenameParameter = RenameParameter.Builder.build,
                              prefixParameter: ColumnPrefixParameter = ColumnPrefixParameter.Builder.build,
                              suffixParameter: ColumnSuffixParameter = ColumnSuffixParameter.Builder.build)
  extends Parameter[DataFrame, DataFrame] with DataFrameEditor[Map[String, String]] {

  lazy val dataFrame = convert(df)

  lazy val columns = dataFrame.columns

  lazy val primaryKeys = {
    val regex = s"^($PrimaryKeyParameter.PK)(.+)" + "$"
    columns
      .map(Pattern.compile(regex).matcher(_))
      .filter(_.find())
      .map(_.group(2))
  }

  lazy val columnMap: Map[String, String] = {
    df.columns
      .map(col => (col, col))
      .map(x => (renameParameter.convert(x._1), x._2))
      .map(x => (litValueParameter.convert(x), x._2))
      .map(x => (primaryKeyParameter.convert(x), x._2))
      .map(x => (prefixParameter.convert(x), x._2))
      .map(x => (suffixParameter.convert(x), x._2))
      .toMap[String, String]
  }

  override def convert(t: DataFrame): DataFrame = {
    logic(t, columnMap)
  }

  override protected def logic(df: DataFrame, t: Map[String, String]): DataFrame = {
    val renamed = df.transform(x => {
      var tmpDf = x
      t.foreach(f => {
        tmpDf = tmpDf.withColumn(f._1, col(f._2))
      })
      tmpDf
    })
    litValueParameter.edit(renamed, t).select(t.keys.toArray.map(col):_*)
  }

}
