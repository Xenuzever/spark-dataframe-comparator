/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package compare


trait Comparator[T, R] {

  def comparing(t1: T, t2: T): R

}
