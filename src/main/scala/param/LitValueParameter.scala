package param

import org.apache.spark.sql.functions._
import editor.DataFrameEditor
import builder.ParameterBuilder
import org.apache.spark.sql.DataFrame

case class LitValueParameter(litColMap: Map[String, String], litValMap: Map[String, String])
  extends ColumnPrefixParameter(litColMap) with DataFrameEditor[Map[String, String]] {

  override protected def logic(df: DataFrame, t: Map[String, String]): DataFrame = {
    df.transform(x => {
      var tmpDf = x
      litColMap.foreach(f => tmpDf.withColumn(f._1, lit(f._2)))
      tmpDf
    })
  }

}

object LitValueParameter {

  final val LIT = "[LIT]"

  object Builder extends ParameterBuilder[(String, String), LitValueParameter] {
    override def build: LitValueParameter = {
      val litColMap = buffer.map(x => (x._1, LIT)).toMap
      val litValMap = buffer.toMap
      LitValueParameter.apply(litColMap, litValMap)
    }
  }

}
