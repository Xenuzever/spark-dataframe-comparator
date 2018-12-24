package param

abstract class Parameter[T, R] {

  def convert(t: T): R

}
