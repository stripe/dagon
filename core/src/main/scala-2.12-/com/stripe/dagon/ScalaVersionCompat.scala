package com.stripe.dagon

object ScalaVersionCompat {
  type LazyList[+A] = scala.collection.immutable.Stream[A]
  val LazyList = scala.collection.immutable.Stream

  def lazyList[A](as: A*): LazyList[A] =
    Stream(as: _*)

  def lazyListToIterator[A](lst: LazyList[A]): Iterator[A] =
    lst.iterator

  def lazyListFromIterator[A](it: Iterator[A]): LazyList[A] =
    it.toStream

  implicit val ieeeDoubleOrdering: Ordering[Double] =
    Ordering.Double
}
