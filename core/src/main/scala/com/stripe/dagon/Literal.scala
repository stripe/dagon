package com.stripe.dagon

/**
 * This represents literal expressions (no variable redirection)
 * of container nodes of type N[T]
 */
sealed trait Literal[T, N[_]] {
  def evaluate: N[T] = Literal.evaluate(this)
}
case class ConstLit[T, N[_]](override val evaluate: N[T]) extends Literal[T, N]
case class UnaryLit[T1, T2, N[_]](arg: Literal[T1, N],
    fn: N[T1] => N[T2]) extends Literal[T2, N] {
}
case class BinaryLit[T1, T2, T3, N[_]](arg1: Literal[T1, N], arg2: Literal[T2, N],
    fn: (N[T1], N[T2]) => N[T3]) extends Literal[T3, N] {
}

object Literal {
  /**
   * This evaluates a literal formula back to what it represents
   * being careful to handle diamonds by creating referentially
   * equivalent structures (not just structurally equivalent)
   */
  def evaluate[T, N[_]](lit: Literal[T, N]): N[T] =
    evaluate(HMap.empty[({ type L[T] = Literal[T, N] })#L, N], lit)._2

  // Memoized version of the above to handle diamonds
  private def evaluate[T, N[_]](hm: HMap[({ type L[T1] = Literal[T1, N] })#L, N], lit: Literal[T, N]): (HMap[({ type L[T1] = Literal[T1, N] })#L, N], N[T]) =
    hm.get(lit) match {
      case Some(prod) => (hm, prod)
      case None =>
        lit match {
          case ConstLit(prod) => (hm + (lit -> prod), prod)
          case UnaryLit(in, fn) =>
            val (h1, p1) = evaluate(hm, in)
            val p2 = fn(p1)
            (h1 + (lit -> p2), p2)
          case BinaryLit(in1, in2, fn) =>
            val (h1, p1) = evaluate(hm, in1)
            val (h2, p2) = evaluate(h1, in2)
            val p3 = fn(p1, p2)
            (h2 + (lit -> p3), p3)
        }
    }
}

