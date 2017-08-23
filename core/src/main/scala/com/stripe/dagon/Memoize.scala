package com.stripe.dagon

object Memoize {

  /**
   * Allow the user to create memoized recursive functions, by
   * providing a function which can operate values as well as
   * references to "itself".
   *
   * For example, we can translate the naive recursive Fibonnaci
   * definition (which is exponential) into an opimized linear-time
   * (and linear-space) form:
   *
   *   Memoize.function[Int, Long] {
   *     case (0, _) => 1
   *     case (1, _) => 1
   *     case (i, f) => f(i - 1) + f(i - 2)
   *   }
   */
  def function[A, B](f: (A, A => B) => B): A => B = {
    // It is tempting to use a mutable.Map here,
    // but mutating the Map inside of the call-by-name value causes
    // some issues in some versions of scala. It is
    // safer to use a mutable pointer to an immutable Map.
    var cache = Map.empty[A, B]
    def getOrElseUpdate(a: A, b: => B): B =
      cache.get(a) match {
        case Some(res) => res
        case None =>
          val res = b
          cache = cache.updated(a, res)
          res
      }

    lazy val g: A => B = (a: A) => getOrElseUpdate(a, f(a, g))
    g
  }

  type RecursiveK[A[_], B[_]] = FunctionK[Lambda[x => (A[x], FunctionK[A, B])], B]

  /**
   * Memoize a FunctionK using an HCache internally.
   */
  def functionK[A[_], B[_]](f: RecursiveK[A, B]): FunctionK[A, B] = {
    val hcache = HCache.empty[A, B]
    lazy val hg: FunctionK[A, B] = new FunctionK[A, B] {
      def toFunction[T] = { at =>
        hcache.getOrElseUpdate(at, f((at, hg)))
      }
    }
    hg
  }
}
