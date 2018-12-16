package info

import com.univocity.parsers.annotations.Replace
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}

import scala.collection.mutable.ArrayBuffer

class ColumnInfo extends ComparingInfo[Column] {


}

case class Column(name: String, replace: Replace = new Replace) {

  val prefix = new Prefix

  val suffix = new Suffix

  lazy val fullName = prefix + name + suffix

  class Prefix extends Affix {
    override protected def apply(str: String) = toString + str
  }

  class Suffix extends Affix {
    override protected def apply(str: String) = str + toString
  }

  private trait Affix {

    private val buf = ArrayBuffer.empty[String]

    def append(aff: String): Unit = buf.append(aff)

    protected abstract def apply(str: String): String

    override def toString = buf.toString()

  }

}

case class Replace(before: String = "", after: String = "") {

  val apply = udf((value: String, before: String, after: String) => {
    value.replace(before, after)
  })

  val applyValueToLit = udf((value: String, after: String) => {
    apply(col(value), lit(value), lit(after))
  })

}
