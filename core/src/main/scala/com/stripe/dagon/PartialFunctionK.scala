package com.stripe.dagon

/**
 * This is a partial Natural transformation.
 *
 * For any type X, this type can produce a partial function from T[X]
 * to R[X].
 */
trait PartialFunctionK[T[_], R[_]] {
  def apply[U]: PartialFunction[T[U], R[U]]
}
