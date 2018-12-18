package info

abstract class Information[T](t: T) {

  final def getData: T = t

  protected def concatenate(args: String*): String = {
    args.mkString
  }

}
