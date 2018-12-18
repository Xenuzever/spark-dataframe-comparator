package info

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class DataFrameInfo(name: String, df: DataFrame)
  extends Information[(String, DataFrame)](name, df) {

  final val PK = "[PK]"

  final val LIT = "[LIT]"

  private final val atomicCols = df.columns

  private val pkBuf = new ArrayBuffer[String]

  private var modColMap: mutable.LinkedHashMap[String, String] = new mutable.LinkedHashMap[String, String]

  private var litValMap: Map[String, Any] = Map.empty

  private var renameMap: Map[String, String] = Map.empty

  private var copyMap: Map[String, String] = Map.empty

  private var prefixMap: Map[String, ArrayBuffer[String]] = Map.empty

  private var suffixMap: Map[String, ArrayBuffer[String]] = Map.empty

  private def toDfColName(column: String) = {
    concatenate(name, ".", column)
  }

  def make(): DataFrame = {

    modColMap = new mutable.LinkedHashMap[String, String]

    Array.concat(

      // PK & LIT
      atomicCols.map(col => {
        if (pkBuf.contains(col)) (PK + col, col)
        else if (litValMap.contains(col)) (LIT + col, col)
        else (col, col)
      }),

      // ONLY LIT
      litValMap.filterNot(m => atomicCols.contains(m._1))
        .map(m => (LIT + m._1, m._1))
        .toArray,

      // COPY
      atomicCols.filter(col => copyMap.contains(col))
        .map(col => (copyMap.get(col).get, col)),

      // RENAME
      atomicCols.filter(col => renameMap.contains(col))
        .map(col => (renameMap.get(col).get, col)),

      // APPLY
      modColMap
        .filter(m => prefixMap.contains(m._2) || suffixMap.contains(m._2))
        .map(m => (m,
          prefixMap.get(m._2).getOrElse(ArrayBuffer.empty),
          suffixMap.get(m._2).getOrElse(ArrayBuffer.empty)))
        .map(f => (f._2.mkString + f._1._1 + f._3.mkString, f._1._2))
        .map(x => (x._1 -> x._2))
        .toArray

    )
      .foreach(x => {
        modColMap.put(x._1, x._2)
      })

    // Remove renamed columns.
    renameMap.keys.foreach(modColMap.remove(_))

    // MAKE DF
    df.transform(x => {
      var tmpDf = x
      modColMap
        .foreach(m => {
          if (m._1.contains(LIT)) tmpDf = tmpDf.withColumn(m._1, lit(litValMap.get(m._2).get))
          else if (renameMap.contains(m._2)) tmpDf = tmpDf.withColumnRenamed(m._2, m._1)
          else tmpDf = tmpDf.withColumn(m._1, col(m._2))
        })
      tmpDf
    })
      .select(modColMap.keys.map(col(_)).toSeq:_*)

  }

  def appendPrefix(column: String, prefix: String): DataFrameInfo = {
    if (!prefixMap.contains(column)) {
      prefixMap = prefixMap + (column -> new ArrayBuffer[String])
    }
    prefixMap.get(column).get.append(prefix)
    this
  }

  def appendSuffix(column: String, suffix: String): DataFrameInfo = {
    if (!suffixMap.contains(column)) {
      suffixMap = suffixMap + (column -> new ArrayBuffer[String])
    }
    suffixMap.get(column).get.append(suffix)
    this
  }

  def appendLitValue(column: String, value: Any): DataFrameInfo = {
    litValMap = litValMap + (column -> value)
    this
  }

  def columnRename(before: String, after: String): DataFrameInfo = {
    renameMap = renameMap + (before -> after)
    this
  }

  def copyColumn(from: String, to: String): DataFrameInfo = {
    copyMap = copyMap + (from -> to)
    this
  }

  def appendPrimaryKey(primaryKey: String): DataFrameInfo = {
    pkBuf.append(primaryKey)
    this
  }

  final def getPrimaryKeys = pkBuf.toArray

  final def getAtomicColumns = atomicCols

  final def getColumnMap = modColMap

  final def getColumns = modColMap.keys

}
