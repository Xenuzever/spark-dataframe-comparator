package information

import java.util.regex.Pattern

import DataFrameInformation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

case class DataFrameInformation(name: String, df: DataFrame)
  extends Information[(String, DataFrame)](name, df) with Creator[DataFrameInformation, DataFrame]  {

  lazy val createdDf = create

  private final val atomicCols = df.columns

  private final var pkMap: Map[String, String] = Map.empty

  private final var litValMap: Map[String, Any] = Map.empty

  private final var renameMap: Map[String, String] = Map.empty

  private final var copyMap: Map[String, String] = Map.empty

  private final var prefixMap: Map[String, ArrayBuffer[String]] = Map.empty

  private final var suffixMap: Map[String, ArrayBuffer[String]] = Map.empty

  private final lazy val modifiedColMap: Map[String, String] = {
    atomicCols
      .map(col => (col, col))
      .map(x => (pkMap.get(x._2).getOrElse(x._1), x._2))
      .map(x => (copyMap.get(x._2).getOrElse(x._1), x._2))
      .map(x => (renameMap.get(x._2).getOrElse(x._1), x._2))
      .map(x => {
        val prefix = prefixMap.get(x._1).getOrElse(ArrayBuffer.empty).mkString
        val suffix = suffixMap.get(x._1).getOrElse(ArrayBuffer.empty).mkString
        (prefix + x._1 + suffix, x._2)
      })
      .map(x => (addColumnToOwnerName(x._1), x._2))
      .toMap[String, String]
  }

  def appendPrefix(column: String, prefix: String): DataFrameInformation = {
    if (!prefixMap.contains(column)) {
      prefixMap = prefixMap + (column -> new ArrayBuffer[String])
    }
    prefixMap.get(column).get.append(prefix)
    this
  }

  def appendSuffix(column: String, suffix: String): DataFrameInformation = {
    if (!suffixMap.contains(column)) {
      suffixMap = suffixMap + (column -> new ArrayBuffer[String])
    }
    suffixMap.get(column).get.append(suffix)
    this
  }

  def appendLitValue(column: String, value: Any): DataFrameInformation = {
    litValMap = litValMap + (column -> value)
    this
  }

  def columnRename(before: String, after: String): DataFrameInformation = {
    renameMap = renameMap + (before -> after)
    this
  }

  def copyColumn(from: String, to: String): DataFrameInformation = {
    copyMap = copyMap + (from -> to)
    this
  }

  def appendPrimaryKey(primaryKey: String): DataFrameInformation = {
    pkMap = pkMap + (primaryKey -> (PK + primaryKey))
    this
  }

  final def addColumnToOwnerName(colName: String): String = {
    name + OWNER_SEPARATOR + colName
  }

  final def removeOwnerNameForColumn(colName: String): String = {
    val matcher = havingOwnerMatcher(colName)
    if (matcher.find()) matcher.group(2)
    else colName
  }

  protected final def havingOwnerMatcher(colName: String) = {
    Pattern.compile("^(" + name + ")" + OWNER_SEPARATOR + "(.+)$").matcher(colName)
  }

  final def getAtomicColumns = {
    atomicCols
  }

  final def getModifiedColumnMap = {
    modifiedColMap
  }

  final def getColumnMap = {
    modifiedColMap.map(x  => (removeOwnerNameForColumn(x._1), x._1))
  }

  final def getSimplePrimaryKeySet = {
    getPrimaryKeySet.map(removeOwnerNameForColumn)
  }

  final def getPrimaryKeySet = {
    modifiedColMap.filter(_._1.contains(PK)).keySet
  }

  final def getPrimaryKeyMap = {
    modifiedColMap.filter(_._1.contains(PK))
  }

  final def getColumnSet = {
    modifiedColMap.keySet
  }

  override protected def create: DataFrame = {
    df.transform(x => {
      var tmpDf = x
      modifiedColMap
        .foreach(m =>
          tmpDf = tmpDf.
            withColumn(m._1, lit(litValMap.get(removeOwnerNameForColumn(m._1)).getOrElse(col(m._2))))
        )
      tmpDf
    })
      .select(modifiedColMap.keys.toArray.map(col):_*)
  }

  override protected def clear: Unit = {
    pkMap = Map.empty
    litValMap = Map.empty
    renameMap = Map.empty
    copyMap = Map.empty
    prefixMap = Map.empty
    suffixMap = Map.empty
  }

  override def toString: String = name

}

object DataFrameInformation {

  final val PK = "[PK]"

  final val OWNER_SEPARATOR = "-"

}
