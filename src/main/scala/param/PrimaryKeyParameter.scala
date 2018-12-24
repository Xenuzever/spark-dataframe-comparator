/*
 *
 *   Copyright 2018 Xena.
 *
 *   This software is released under the MIT License.
 *   http://opensource.org/licenses/mit-license.php
 *
 */

package param

import builder.ParameterBuilder

case class PrimaryKeyParameter(columns: Array[String]) extends Parameter[(String, String), String] {

  override def convert(t: (String, String)): String = {
    if (columns.contains(t._2)) PrimaryKeyParameter.PK + t._1 else t._1
  }

}

object PrimaryKeyParameter {

  final val PK = "[PK]"

}

class PrimaryKeyParameterBuilder extends ParameterBuilder[String, PrimaryKeyParameter] {

  override def build: PrimaryKeyParameter = PrimaryKeyParameter.apply(buffer.toArray)

}