package comparator


trait Comparator[T, R] {

  def comparing(t1: T, t2: T): R

}
