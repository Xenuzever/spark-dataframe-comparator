package information

trait Creator[U <: Information[_], R] {

  protected def create: R

}
