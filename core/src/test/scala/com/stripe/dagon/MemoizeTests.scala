package com.stripe.dagon

import org.scalatest.FunSuite

class MemoizeTests extends FunSuite {
  test("fibonacci is linear in time") {

    var calls = 0

    val fib =
      Memoize.function[Int, Long] { (i, f) =>
        calls += 1

        i match {
          case 0 => 0
          case 1 => 1
          case i => f(i - 1) + f(i - 2)
        }
      }

    lazy val streamFib: Stream[Long] =
      0L #:: 1L #:: (streamFib.zip(streamFib.drop(1)).map { case (a, b) => a + b })

    assert(fib(100) == streamFib(100))
    assert(calls == 101)
  }

  test("functionK repeated calls only evaluate once") {

    var calls = 0
    val fn =
      Memoize.functionK[BoolT, BoolT](new Memoize.RecursiveK[BoolT, BoolT] {
        def toFunction[T] = { case (b, rec) =>
          calls += 1

          !b
        }
      })

    assert(fn(true) == false)
    assert(calls == 1)
    assert(fn(true) == false)
    assert(calls == 1)

    assert(fn(false) == true)
    assert(calls == 2)
    assert(fn(false) == true)
    assert(calls == 2)

  }
}
