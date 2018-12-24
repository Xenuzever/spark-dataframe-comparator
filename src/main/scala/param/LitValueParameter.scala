package param

import org.apache.spark.sql.functions._
import builder.ParameterBuilder
import org.apache.spark.sql.DataFrame

case class LitValueParameter(litValMap: Map[String, String]) extends Parameter[DataFrame, DataFrame] {

  override def convert(t: DataFrame): DataFrame = {
    t.transform(x => {
      var tmpDf = x
      litValMap.foreach(f => {
        tmpDf = tmpDf.withColumn(f._1, lit(f._2))
      })
      tmpDf
    })
  }

}

object LitValueParameter {

  final val LIT = "[LIT]"

}

class LitValueParameterBuilder extends ParameterBuilder[(String, String), LitValueParameter] {

  override def build: LitValueParameter = {
    val litValMap = buffer.toMap
    LitValueParameter.apply(litValMap)
  }

}
