package com.stripe.dagon

/**
 * This represents literal expressions (no variable redirection)
 * of container nodes of type N[T]
 */
sealed trait Literal[N[_], T] {
  def evaluate: N[T] = Literal.evaluate(this)
}

object Literal {

  case class Const[N[_], T](override val evaluate: N[T]) extends Literal[N, T] {
    // cache hashCode since we hit it so much.
    override val hashCode = (getClass, evaluate).hashCode

    // We often will cache the N[T] -> Literal[N, T] mapping, so we
    // deal with referentially equal things, which can give cheap equality
    override def equals(that: Any) =
      that match {
        case thatAR: AnyRef if thatAR eq this => true
        case Const(thatE) => evaluate == thatE
        case _ => false
      }
  }

  case class Unary[N[_], T1, T2](arg: Literal[N, T1], fn: N[T1] => N[T2]) extends Literal[N, T2] {
    // cache hashCode since we hit it so much.
    override val hashCode = (getClass, arg, fn).hashCode

    // We often will cache the N[T] -> Literal[N, T] mapping, so we
    // deal with referentially equal things, which can give cheap equality
    override def equals(that: Any) =
      that match {
        case thatAR: AnyRef if thatAR eq this => true
        case Unary(thatA, thatfn) =>
          // check the function first, before recursing on the literal
          (thatfn == fn) && (arg == thatA)
        case _ => false
      }
  }

  case class Binary[N[_], T1, T2, T3](arg1: Literal[N, T1],
                                      arg2: Literal[N, T2],
                                      fn: (N[T1], N[T2]) => N[T3]) extends Literal[N, T3] {
    // cache hashCode since we hit it so much.
    override val hashCode = (getClass, arg1, arg2, fn).hashCode

    // We often will cache the N[T] -> Literal[N, T] mapping, so we
    // deal with referentially equal things, which can give cheap equality
    override def equals(that: Any) =
      that match {
        case thatAR: AnyRef if thatAR eq this => true
        case Binary(thatA1, thatA2, thatfn) =>
          // check the function first, before recursing on the literal
          (thatfn == fn) && (arg1 == thatA1) && (arg2 == thatA2)
        case _ => false
      }
  }

  case class Variadic[N[_], T1, T2](args: List[Literal[N, T1]], fn: List[N[T1]] => N[T2]) extends Literal[N, T2]{
    // cache hashCode since we hit it so much.
    override val hashCode = (getClass, args, fn).hashCode
    // We often will cache the N[T] -> Literal[N, T] mapping, so we
    // deal with referentially equal things, which can give cheap equality
    override def equals(that: Any) =
      that match {
        case thatAR: AnyRef if thatAR eq this => true
        case Variadic(thatArgs, thatfn) =>
          // check the function first, before recursing on the literal
          (thatfn == fn) && (args == thatArgs)
        case _ => false
      }
  }

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
        case (Variadic(args, fn), rec) => fn(args.map(rec(_)))
      }
    })
}
