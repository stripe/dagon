package com.stripe.dagon

/**
 * This implements a simplification rule on ExpressionDags
 */
trait Rule[N[_]] { self =>

  /**
   * If the given Id can be replaced with a simpler expression,
   * return Some(expr) else None.
   *
   * If it is convenient, you might write a partial function
   * and then call .lift to get the correct Function type
   */
  def apply[T](on: ExpressionDag[N]): N[T] => Option[N[T]]

  // If the current rule cannot apply, then try the argument here
  def orElse(that: Rule[N]): Rule[N] = new Rule[N] {
    def apply[T](on: ExpressionDag[N]) = { n =>
      self.apply(on)(n).orElse(that.apply(on)(n))
    }

    override def toString: String =
      s"$self.orElse($that)"
  }
}

object Rule {
  def empty[N[_]]: Rule[N] =
    new Rule[N] {
      def apply[T](on: ExpressionDag[N]) = { _ => None }
    }
}
