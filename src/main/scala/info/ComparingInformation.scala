package info

import scala.collection.mutable.ArrayBuffer

class ComparingInformation[T, U](t1: T, t2: T) extends Information[(T, T)](t1, t2) {

  private final val pairs = new ArrayBuffer[(U, U)]

  def appendPair[R >: ComparingInformation[T, U]](u1: U, u2: U): R = {
    pairs.append((u1, u2))
    this
  }

  def removePair(index: Int) = {
    pairs.remove(index)
  }

  def getPairs = pairs.toArray

  override protected def clear: Unit = {
    pairs.clear()
  }

}
