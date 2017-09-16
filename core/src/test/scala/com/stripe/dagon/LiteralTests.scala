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
import org.scalacheck.{Arbitrary, Gen, Properties}

import Literal.{Binary, Const, Unary, Variadic}

object LiteralTests extends Properties("Literal") {
  case class Box[T](get: T)

  def transitiveClosure[N[_]](
      l: Literal[N, _],
      acc: Set[Literal[N, _]] = Set.empty[Literal[N, _]]): Set[Literal[N, _]] = l match {
    case c @ Const(_) => acc + c
    case u @ Unary(prev, _) => if (acc(u)) acc else transitiveClosure(prev, acc + u)
    case b @ Binary(p1, p2, _) =>
      if (acc(b)) acc else transitiveClosure(p2, transitiveClosure(p1, acc + b))
    case v @ Variadic(ins, fn) =>
      val newNodes = ins.filterNot(acc)
      newNodes.foldLeft(acc + v) { (res, n) => transitiveClosure(n, res) }
  }

  def genBox: Gen[Box[Int]] = Gen.chooseNum(Int.MinValue, Int.MaxValue).map(Box(_))

  def genConst: Gen[Literal[Box, Int]] = genBox.map(Const(_))
  def genUnary: Gen[Literal[Box, Int]] =
    for {
      fn <- Arbitrary.arbitrary[(Int) => (Int)]
      bfn = { case Box(b) => Box(fn(b)) }: Box[Int] => Box[Int]
      input <- genLiteral
    } yield Unary(input, bfn)

  def genBinary: Gen[Literal[Box, Int]] =
    for {
      fn <- Arbitrary.arbitrary[(Int, Int) => (Int)]
      bfn = { case (Box(l), Box(r)) => Box(fn(l, r)) }: (Box[Int], Box[Int]) => Box[Int]
      left <- genLiteral
      // We have to make dags, so select from the closure of left sometimes
      right <- Gen.oneOf(genLiteral, genChooseFrom(transitiveClosure[Box](left)))
    } yield Binary(left, right, bfn)

  def genVariadic: Gen[Literal[Box, Int]] = {
    def append(cnt: Int, items: List[Literal[Box, Int]]): Gen[List[Literal[Box, Int]]] =
      if (cnt > 0) {

        val hGen: Gen[Literal[Box, Int]] =
          if (items.nonEmpty) {
            val inner = Gen.oneOf(items.flatMap(transitiveClosure[Box](_)))
              .asInstanceOf[Gen[Literal[Box, Int]]]
            Gen.frequency((4, Gen.lzy(genLiteral)), (1, inner))
          } else Gen.lzy(genLiteral)

        for {
          head <- hGen
          rest <- append(cnt - 1, head :: items)
        } yield rest
      }
      else Gen.const(items)

    for {
      argc <- Gen.choose(0, 4)
      args <- append(argc, Nil)
      fn <- Arbitrary.arbitrary[List[Int] => Int]
      bfn = { boxes: List[Box[Int]] => Box(fn(boxes.map { case Box(b) => b })) }
    } yield Variadic(args, bfn)
  }

  def genChooseFrom[N[_]](s: Set[Literal[N, _]]): Gen[Literal[N, Int]] =
    Gen.oneOf(s.toSeq.asInstanceOf[Seq[Literal[N, Int]]])

  /*
   * Create dags. Don't use binary too much as it can create exponentially growing dags
   */
  def genLiteral: Gen[Literal[Box, Int]] =
    Gen.frequency((6, genConst), (12, genUnary), (2, genBinary), (1, genVariadic))

  //This evaluates by recursively walking the tree without memoization
  //as lit.evaluate should do
  def slowEvaluate[T](lit: Literal[Box, T]): Box[T] = lit match {
    case Const(n) => n
    case Unary(in, fn) => fn(slowEvaluate(in))
    case Binary(a, b, fn) => fn(slowEvaluate(a), slowEvaluate(b))
    case Variadic(ins, fn) => fn(ins.map(slowEvaluate(_)))
  }

  property("Literal.evaluate must match simple explanation") = forAll(genLiteral) {
    (l: Literal[Box, Int]) =>
      l.evaluate == slowEvaluate(l)
  }
}
