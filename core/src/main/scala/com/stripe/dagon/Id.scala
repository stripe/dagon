package com.stripe.dagon

/**
 * The Expressions are assigned Ids. Each Id is associated with
 * an expression of inner type T.
 *
 * This is done to put an indirection in the ExpressionDag that
 * allows us to rewrite nodes by simply replacing the expressions
 * associated with given Ids.
 *
 * T is a phantom type used by the type system
 */
final case class Id[T](id: Int)

object Id {
  implicit def idOrdering[T]: Ordering[Id[T]] =
    new Ordering[Id[T]] {
      def compare(a: Id[T], b: Id[T]) =
        Integer.compare(a.id, b.id)
    }
}
