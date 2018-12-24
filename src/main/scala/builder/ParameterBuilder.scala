/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package builder

import scala.collection.mutable.ArrayBuffer

trait ParameterBuilder[T, U] {

  protected val buffer = new ArrayBuffer[T]

  def append(t: T): ParameterBuilder[T, U] = {
    buffer.append(t)
    this
  }

  def appendAll(t: TraversableOnce[T]): ParameterBuilder[T, U] = {
    buffer.appendAll(t)
    this
  }

  def clear: Unit = buffer.clear()

  def build: U

}
