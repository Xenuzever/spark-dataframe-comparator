package info

import scala.collection.mutable.ArrayBuffer

case class PrimaryKeysInfo(df1Name: String, df2Name: String)
  extends Information[(String, String)](df1Name, df2Name) {

  val compBuf = new ArrayBuffer[(String, String)]

  def appendComparingKeyPair(df1Key: String, df2Key: String): PrimaryKeysInfo = {
    compBuf.append((df1Key, df2Key))
    this
  }

  def setComparingKeyPairs(keys: Array[(String, String)]): Unit = {
    compBuf.clear()
    compBuf.appendAll(keys)
  }

  def getPrimaryKeyPairs = {
    compBuf.toArray
  }

}
