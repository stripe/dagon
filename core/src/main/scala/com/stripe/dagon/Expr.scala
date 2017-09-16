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

/**
 * Expr[N, T] is an expression of a graph of container nodes N[_] with
 * result type N[T]. These expressions are like the Literal[T, N] graphs
 * except that functions always operate with an indirection of a Id[T]
 * where N[T] is the type of the input node.
 *
 * Nodes can be deleted from the graph by replacing an Expr at Id = idA
 * with Var(idB) pointing to some upstream node.
 *
 * To add nodes to the graph, add depth to the final node returned in
 * a Unary or Binary expression.
 *
 * TODO: see the approach here: https://gist.github.com/pchiusano/1369239
 * Which seems to show a way to do currying, so we can handle general
 * arity
 */
sealed trait Expr[N[_], T] {
  def evaluate(idToExp: HMap[Id, Expr[N, ?]]): N[T] =
    Expr.evaluate(idToExp, this)

  final def isVar: Boolean =
    this match {
      case Expr.Var(_) => true
      case _ => false
    }
}

object Expr {

  case class Const[N[_], T](value: N[T]) extends Expr[N, T] {
    override def evaluate(idToExp: HMap[Id, Expr[N, ?]]): N[T] =
      value
  }

  case class Var[N[_], T](name: Id[T]) extends Expr[N, T]

  case class Unary[N[_], T1, T2](arg: Id[T1], fn: N[T1] => N[T2]) extends Expr[N, T2]

  case class Binary[N[_], T1, T2, T3](arg1: Id[T1], arg2: Id[T2], fn: (N[T1], N[T2]) => N[T3])
      extends Expr[N, T3]

  case class Variadic[N[_], T1, T2](args: List[Id[T1]], fn: List[N[T1]] => N[T2]) extends Expr[N, T2]

  /**
   * Evaluate the given expression with the given mapping of Id to Expr.
   */
  def evaluate[N[_], T](idToExp: HMap[Id, Expr[N, ?]], expr: Expr[N, T]): N[T] =
    evaluateMemo(idToExp)(expr)

  /**
   * Build a memoized FunctionK for this particular idToExp. Clearly, this
   * FunctionK is only valid for the given idToExp which is captured in this
   * closure.
   */
  def evaluateMemo[N[_]](idToExp: HMap[Id, Expr[N, ?]]): FunctionK[Expr[N, ?], N] =
    Memoize.functionK[Expr[N, ?], N](new Memoize.RecursiveK[Expr[N, ?], N] {
      def toFunction[T] = {
        case (Const(n), _) => n
        case (Var(id), rec) => rec(idToExp(id))
        case (Unary(id, fn), rec) =>
          fn(rec(idToExp(id)))
        case (Binary(id1, id2, fn), rec) =>
          fn(rec(idToExp(id1)), rec(idToExp(id2)))
        case (Variadic(args, fn), rec) =>
          fn(args.map { id => rec(idToExp(id)) })
      }
    })
}
