package com.stripe.dagon

/**
 * This represents literal expressions (no variable redirection)
 * of container nodes of type N[T]
 */
sealed trait Literal[N[_], T] {
  def evaluate: N[T] = Literal.evaluate(this)
}


object Literal {

  case class Const[N[_], T](override val evaluate: N[T])
      extends Literal[N, T]

  case class Unary[N[_], T1, T2](arg: Literal[N, T1], fn: N[T1] => N[T2])
      extends Literal[N, T2]

  case class Binary[N[_], T1, T2, T3](arg1: Literal[N, T1], arg2: Literal[N, T2], fn: (N[T1], N[T2]) => N[T3])
      extends Literal[N, T3]

  /**
   * This evaluates a literal formula back to what it represents
   * being careful to handle diamonds by creating referentially
   * equivalent structures (not just structurally equivalent)
   */
  def evaluate[N[_], T](lit: Literal[N, T]): N[T] =
    evaluate(HMap.empty[Literal[N, ?], N], lit)._2

  // Memoized version of the above to handle diamonds
  private def evaluate[N[_], T](hm: HMap[Literal[N, ?], N], lit: Literal[N, T]): (HMap[Literal[N, ?], N], N[T]) =
    hm.get(lit) match {
      case Some(prod) => (hm, prod)
      case None =>
        lit match {
          case Const(prod) => (hm + (lit -> prod), prod)
          case Unary(in, fn) =>
            val (h1, p1) = evaluate(hm, in)
            val p2 = fn(p1)
            (h1 + (lit -> p2), p2)
          case Binary(in1, in2, fn) =>
            val (h1, p1) = evaluate(hm, in1)
            val (h2, p2) = evaluate(h1, in2)
            val p3 = fn(p1, p2)
            (h2 + (lit -> p3), p3)
        }
    }
}

