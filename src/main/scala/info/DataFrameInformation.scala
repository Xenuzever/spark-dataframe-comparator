package info

import DataFrameInformation._

import creator.Creator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class DataFrameInformation(name: String, df: DataFrame)
  extends Information[(String, DataFrame)](name, df) with Creator[DataFrameInformation, DataFrame]  {

  private final val atomicCols = df.columns

  private val pkBuf = new ArrayBuffer[String]

  private var litValMap: Map[String, Any] = Map.empty

  private var renameMap: Map[String, String] = Map.empty

  private var copyMap: Map[String, String] = Map.empty

  private var prefixMap: Map[String, ArrayBuffer[String]] = Map.empty

  private var suffixMap: Map[String, ArrayBuffer[String]] = Map.empty

  private lazy val modifiedColMap: mutable.LinkedHashMap[String, String] = {

    val modColMap = new mutable.LinkedHashMap[String, String]

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
        .map(col => (renameMap.get(col).get, col))

    )
      .foreach(x => {
        modColMap.put(x._1, x._2)
      })

    // APPLY
    modColMap
      .filter(m => prefixMap.contains(m._2) || suffixMap.contains(m._2))
      .map(m => (m,
        prefixMap.get(m._2).getOrElse(ArrayBuffer.empty),
        suffixMap.get(m._2).getOrElse(ArrayBuffer.empty)))
      .map(f => {
        modColMap.remove(f._1._1)
        (f._2.mkString + f._1._1 + f._3.mkString, f._1._2)
      })
      .foreach(x => modColMap.put(x._1, x._2))

    // Remove renamed columns.
    renameMap.keys.foreach(modColMap.remove(_))

    modColMap

  }

  lazy val createdDf = create

  override protected def create: DataFrame = {

    // MAKE DF
    df.transform(x => {
      var tmpDf = x
      modifiedColMap
        .foreach(m => {
          if (m._1.contains(LIT)) tmpDf = tmpDf.withColumn(m._1, lit(litValMap.get(m._2).get))
//          else if (renameMap.contains(m._2)) tmpDf = tmpDf.withColumnRenamed(m._2, m._1)
          else tmpDf = tmpDf.withColumn(m._1, col(m._2))
        })
      tmpDf
    })
      .select(modifiedColMap.keys.map(col(_)).toSeq:_*)

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
    pkBuf.append(primaryKey)
    this
  }

  final def getAtomicColumns = atomicCols

  final def getPrimaryKeySet = modifiedColMap.filter(_._1.contains(PK)).keySet

  final def getPrimaryKeyMap = modifiedColMap.filter(_._1.contains(PK)).keySet

  final def getColumnSet = modifiedColMap.keySet

  override protected def clear: Unit = {
    pkBuf.clear()
    litValMap = Map.empty
    renameMap = Map.empty
    copyMap = Map.empty
    prefixMap = Map.empty
    suffixMap = Map.empty
  }

}

object DataFrameInformation {

  final val PK = "[PK]"

  final val LIT = "[LIT]"

}
