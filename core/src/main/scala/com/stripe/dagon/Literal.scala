package com.stripe.dagon

/**
 * This represents literal expressions (no variable redirection)
 * of container nodes of type N[T]
 */
sealed trait Literal[N[_], T] {
  def evaluate: N[T] = Literal.evaluate(this)
}

object Literal {

  case class Const[N[_], T](override val evaluate: N[T]) extends Literal[N, T]

  case class Unary[N[_], T1, T2](arg: Literal[N, T1], fn: N[T1] => N[T2]) extends Literal[N, T2]

  case class Binary[N[_], T1, T2, T3](arg1: Literal[N, T1],
                                      arg2: Literal[N, T2],
                                      fn: (N[T1], N[T2]) => N[T3])
      extends Literal[N, T3]

  /**
   * This evaluates a literal formula back to what it represents
   * being careful to handle diamonds by creating referentially
   * equivalent structures (not just structurally equivalent)
   */
  def evaluate[N[_], T](lit: Literal[N, T]): N[T] =
    evaluateMemo[N](lit)

  /**
   * Memoized version of evaluation to handle diamonds
   *
   * Each call to this creates a new internal memo.
   */
  def evaluateMemo[N[_]]: FunctionK[Literal[N, ?], N] =
    Memoize.functionK[Literal[N, ?], N](new Memoize.RecursiveK[Literal[N, ?], N] {
      def toFunction[T] = {
        case (Const(n), _) => n
        case (Unary(n, fn), rec) => fn(rec(n))
        case (Binary(n1, n2, fn), rec) => fn(rec(n1), rec(n2))
      }
    })
}
