package result

abstract class Result[T, R <: ResultData] {

  def analyze(t: T): R

}

abstract class ResultData {
  /** EMPTY */
}
