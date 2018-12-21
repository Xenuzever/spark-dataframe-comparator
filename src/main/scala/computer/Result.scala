package computer

abstract case class Result[T, U <: Logic[T]](t: T) {

  protected val logic: U

  def get = logic.compute(t)

}
