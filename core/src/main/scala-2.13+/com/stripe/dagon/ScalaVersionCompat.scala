package com.stripe.dagon

object ScalaVersionCompat {
  type LazyList[+A] = scala.collection.immutable.LazyList[A]
  val LazyList = scala.collection.immutable.LazyList

  def lazyList[A](as: A*): LazyList[A] =
    LazyList(as: _*)

  def lazyListToIterator[A](lst: LazyList[A]): Iterator[A] =
    lst.iterator

  def lazyListFromIterator[A](it: Iterator[A]): LazyList[A] =
    LazyList.from(it)

  implicit val ieeeDoubleOrdering: Ordering[Double] =
    Ordering.Double.IeeeOrdering
}
