package param

import builder.ParameterBuilder

case class RenameParameter(renameMap: Map[String, String]) extends Parameter[String, String] {

  override def convert(t: String): String = {
    renameMap.get(t).getOrElse(t)
  }

}

class RenameParameterBuilder extends ParameterBuilder[(String, String), RenameParameter] {

  override def build: RenameParameter = RenameParameter.apply(buffer.toMap)

}