package com.stripe.dagon

/**
 * Often a partial function is an easier way to express rules
 */
trait PartialRule[N[_]] extends Rule[N] {
  final def apply[T](on: ExpressionDag[N]) = applyWhere[T](on).lift
  def applyWhere[T](on: ExpressionDag[N]): PartialFunction[N[T], N[T]]
}
