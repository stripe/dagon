package com.stripe.dagon

/**
 * This implements a simplification rule on Dags
 */
trait Rule[N[_]] { self =>

  /**
   * If the given Id can be replaced with a simpler expression,
   * return Some(expr) else None.
   *
   * If it is convenient, you might write a partial function
   * and then call .lift to get the correct Function type
   */
  def apply[T](on: Dag[N]): N[T] => Option[N[T]]

  // If the current rule cannot apply, then try the argument here
  def orElse(that: Rule[N]): Rule[N] =
    new Rule[N] {
      def apply[T](on: Dag[N]) = { n =>
        self.apply(on)(n) match {
          case Some(n1) if n1 == n =>
            // If the rule emits the same as input fall through
            that.apply(on)(n)
          case None =>
            that.apply(on)(n)
          case s@Some(_) => s
        }
      }

      override def toString: String =
        s"$self.orElse($that)"
    }
}

object Rule {
  def empty[N[_]]: Rule[N] =
    new Rule[N] {
      def apply[T](on: Dag[N]) = { _ => None }
    }
}
