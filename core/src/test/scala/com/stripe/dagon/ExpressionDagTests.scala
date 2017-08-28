/*
 Copyright 2014 Twitter, Inc.
 Copyright 2017 Stripe, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.stripe.dagon

import org.scalacheck.Prop._
import org.scalacheck.{Gen, Prop, Properties}

object ExpressionDagTests extends Properties("ExpressionDag") {

  /*
   * Here we test with a simple algebra optimizer
   */

  sealed trait Formula[T] { // we actually will ignore T
    def evaluate: Int
    def closure: Set[Formula[T]]

    def inc(n: Int): Formula[T] = Inc(this, n)
    def +(that: Formula[T]): Formula[T] = Sum(this, that)
    def *(that: Formula[T]): Formula[T] = Product(this, that)
  }

  object Formula {
    def apply(n: Int): Formula[Unit] = Constant(n)

    def inc[T](by: Int): Formula[T] => Formula[T] = Inc(_, by)
    def sum[T]: (Formula[T], Formula[T]) => Formula[T] = Sum(_, _)
    def product[T]: (Formula[T], Formula[T]) => Formula[T] = Product(_, _)
  }

  case class Constant[T](override val evaluate: Int) extends Formula[T] {
    def closure = Set(this)
  }
  case class Inc[T](in: Formula[T], by: Int) extends Formula[T] {
    def evaluate = in.evaluate + by
    def closure = in.closure + this
  }
  case class Sum[T](left: Formula[T], right: Formula[T]) extends Formula[T] {
    def evaluate = left.evaluate + right.evaluate
    def closure = (left.closure ++ right.closure) + this
  }
  case class Product[T](left: Formula[T], right: Formula[T]) extends Formula[T] {
    def evaluate = left.evaluate * right.evaluate
    def closure = (left.closure ++ right.closure) + this
  }

  def testRule[T](start: Formula[T], expected: Formula[T], rule: Rule[Formula]): Prop = {
    val got = ExpressionDag.applyRule(start, toLiteral, rule)
    (got == expected) :| s"$got == $expected"
  }

  def genForm: Gen[Formula[Int]] =
    Gen.frequency((1, genProd), (1, genSum), (4, genInc), (4, genConst))

  def genConst: Gen[Formula[Int]] = Gen.chooseNum(Int.MinValue, Int.MaxValue).map(Constant(_))

  def genInc: Gen[Formula[Int]] =
    for {
      by <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
      f <- Gen.lzy(genForm)
    } yield Inc(f, by)

  def genSum: Gen[Formula[Int]] =
    for {
      left <- Gen.lzy(genForm)
      // We have to make dags, so select from the closure of left sometimes
      right <- Gen.oneOf(genForm, Gen.oneOf(left.closure.toSeq))
    } yield Sum(left, right)

  def genProd: Gen[Formula[Int]] =
    for {
      left <- Gen.lzy(genForm)
      // We have to make dags, so select from the closure of left sometimes
      right <- Gen.oneOf(genForm, Gen.oneOf(left.closure.toSeq))
    } yield Product(left, right)

  /**
   * Here we convert our dag nodes into Literal[Formula, T]
   */
  def toLiteral: FunctionK[Formula, Literal[Formula, ?]] =
    Memoize.functionK[Formula, Literal[Formula, ?]](
      new Memoize.RecursiveK[Formula, Literal[Formula, ?]] {
        def toFunction[T] = {
          case (c @ Constant(_), _) => Literal.Const(c)
          case (Inc(in, by), f) => Literal.Unary(f(in), Formula.inc(by))
          case (Sum(lhs, rhs), f) => Literal.Binary(f(lhs), f(rhs), Formula.sum)
          case (Product(lhs, rhs), f) => Literal.Binary(f(lhs), f(rhs), Formula.product)
        }
      })

  /**
   * Inc(Inc(a, b), c) = Inc(a, b + c)
   */
  object CombineInc extends Rule[Formula] {
    def apply[T](on: ExpressionDag[Formula]) = {
      case Inc(i @ Inc(a, b), c) if on.fanOut(i) == 1 => Some(Inc(a, b + c))
      case _ => None
    }
  }

  object RemoveInc extends PartialRule[Formula] {
    def applyWhere[T](on: ExpressionDag[Formula]) = {
      case Inc(f, by) => Sum(f, Constant(by))
    }
  }

  //Check the Node[T] <=> Id[T] is an Injection for all nodes reachable from the root

  property("toLiteral/Literal.evaluate is a bijection") = forAll(genForm) { form =>
    toLiteral.apply(form).evaluate == form
  }

  property("Going to ExpressionDag round trips") = forAll(genForm) { form =>
    val (dag, id) = ExpressionDag(form, toLiteral)
    dag.evaluate(id) == form
  }

  property("CombineInc does not change results") = forAll(genForm) { form =>
    val simplified = ExpressionDag.applyRule(form, toLiteral, CombineInc)
    form.evaluate == simplified.evaluate
  }

  property("RemoveInc removes all Inc") = forAll(genForm) { form =>
    val noIncForm = ExpressionDag.applyRule(form, toLiteral, RemoveInc)
    def noInc(f: Formula[Int]): Boolean = f match {
      case Constant(_) => true
      case Inc(_, _) => false
      case Sum(l, r) => noInc(l) && noInc(r)
      case Product(l, r) => noInc(l) && noInc(r)
    }
    noInc(noIncForm) && (noIncForm.evaluate == form.evaluate)
  }

  // The normal Inc gen recursively calls the general dag Generator
  def genChainInc: Gen[Formula[Int]] =
    for {
      by <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
      chain <- genChain
    } yield Inc(chain, by)

  def genChain: Gen[Formula[Int]] = Gen.frequency((1, genConst), (3, genChainInc))

  property("CombineInc compresses linear Inc chains") = forAll(genChain) { chain =>
    ExpressionDag.applyRule(chain, toLiteral, CombineInc) match {
      case Constant(n) => true
      case Inc(Constant(n), b) => true
      case _ => false // All others should have been compressed
    }
  }

  /**
   * We should be able to totally evaluate these formulas
   */
  object EvaluationRule extends Rule[Formula] {
    def apply[T](on: ExpressionDag[Formula]) = {
      case Sum(Constant(a), Constant(b)) => Some(Constant(a + b))
      case Product(Constant(a), Constant(b)) => Some(Constant(a * b))
      case Inc(Constant(a), b) => Some(Constant(a + b))
      case _ => None
    }
  }
  property("EvaluationRule totally evaluates") = forAll(genForm) { form =>
    testRule(form, Constant(form.evaluate), EvaluationRule)
  }

  property("Crush down explicit diamond") = forAll { (xs0: List[Int], ys0: List[Int]) =>
    val a = Formula(123)

    // ensure that we won't ever use the same constant on the LHS and RHS
    // because we want all our inc nodes to fan out to only one other node.
    def munge(xs: List[Int]): List[Int] = xs.take(10).map(_ % 10)
    val (xs, ys) = (munge(0 :: xs0), munge(0 :: ys0).map(_ + 1000))
    val (x, y) = (xs.sum, ys.sum)

    val complex = xs.foldLeft(a)(_ inc _) + ys.foldLeft(a)(_ inc _)
    val expected = a.inc(x) + a.inc(y)
    testRule(complex, expected, CombineInc)
  }

  property("all tails have fanOut of 1") = forAll { (n1: Int, ns: List[Int]) =>
    // Make sure we have a set of distinct nodes
    val tails = (n1 :: ns).zipWithIndex.map { case (i, idx) => Formula(i).inc(idx) }

    val (dag, roots) =
      tails.foldLeft((ExpressionDag.empty[Formula](toLiteral), Set.empty[Id[_]])) {
        case ((d, s), f) =>
          val (dnext, id) = d.addRoot(f)
          (dnext, s + id)
      }

    roots.forall(dag.fanOut(_) == 1)
  }
}
