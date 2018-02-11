package com.stripe.dagon

import scala.util.hashing.MurmurHash3
/**
 * A tuple2 that uses reference equality on items to do equality
 * useful for caching the results of pair-wise functions on DAGs.
 *
 * Without this, you can easily get exponential complexity on
 * recursion on DAGs.
 */
case class RefPair[A <: AnyRef, B <: AnyRef](_1: A, _2: B) {
  private[this] var hashCodeVar: Int = 0

  override def hashCode =
    if (hashCodeVar != 0) hashCodeVar
    else {
      val hc0 = MurmurHash3.productHash(this)
      // make sure we don't use 0, which we are using
      // to signal that we have not computed yet
      val hc = if (hc0 == 0) -1 else hc0
      // Store it here, but without thread sync
      // if there are two threads, they may both
      // compute, but hashCode should be stable
      // so that's fine
      hashCodeVar = hc
      hc
    }

  override def equals(that: Any) = that match {
    case RefPair(thatA, thatB) => (_1 eq thatA) && (_2 eq thatB)
    case _ => false
  }

  /**
   * true if the left is referentially equal to the right
   */
  def itemsEq: Boolean = _1 eq _2
}
