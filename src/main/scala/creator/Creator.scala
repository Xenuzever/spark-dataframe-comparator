package creator

import info.Information

trait Creator[U <: Information[_], R] {

  protected def create: R

}
