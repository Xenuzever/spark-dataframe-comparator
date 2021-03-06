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

abstract class AffixParameter(affixMap: Map[String, String])
  extends Parameter[(String, String), String] {
  override def convert(t: (String, String)): String
}

class ColumnPrefixParameter(prefixMap: Map[String, String]) extends AffixParameter(prefixMap) {
  override def convert(t: (String, String)): String = {
    val prefix = prefixMap.get(t._2).getOrElse("")
    prefix + t._1
  }
}

class ColumnSuffixParameter(suffixMap: Map[String, String]) extends AffixParameter(suffixMap) {
  override def convert(t: (String, String)): String = {
    val suffix = suffixMap.get(t._2).getOrElse("")
    t._1 + suffix
  }
}

class ColumnPrefixParameterBuilder extends ParameterBuilder[(String, String), ColumnPrefixParameter] {
  override def build: ColumnPrefixParameter = new ColumnPrefixParameter(buffer.toMap)
}

class ColumnSuffixParameterBuilder extends ParameterBuilder[(String, String), ColumnSuffixParameter] {
  override def build: ColumnSuffixParameter = new ColumnSuffixParameter(buffer.toMap)
}


