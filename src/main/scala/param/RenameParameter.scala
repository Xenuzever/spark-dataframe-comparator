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

class RenameParameter(renameMap: Map[String, String]) extends Parameter[String, String] {

  override def convert(t: String): String = {
    renameMap.get(t).getOrElse(t)
  }

}

class RenameParameterBuilder extends ParameterBuilder[(String, String), RenameParameter] {

  override def build: RenameParameter = new RenameParameter(buffer.toMap)

}