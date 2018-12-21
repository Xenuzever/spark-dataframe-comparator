package create

import information.Information

trait Creator[U <: Information[_], R] {

  protected def create: R

}
