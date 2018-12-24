package result

abstract class Result[T, R <: Result[T, R]] {

  protected def analyze(t: T): R

}
