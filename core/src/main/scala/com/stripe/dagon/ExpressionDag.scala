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

sealed trait ExpressionDag[N[_]] { self =>

  /**
   * These have package visibility to test
   * the law that for all Expr, the node they
   * evaluate to is unique
   */
  protected[dagon] def idToExp: HMap[Id, Expr[N, ?]]
  protected def nodeToLiteral: FunctionK[N, Literal[N, ?]]
  protected def roots: Set[Id[_]]
  protected def nextId: Int

  private def copy(
    id2Exp: HMap[Id, Expr[N, ?]] = self.idToExp,
    node2Literal: FunctionK[N, Literal[N, ?]] = self.nodeToLiteral,
    gcroots: Set[Id[_]] = self.roots,
    id: Int = self.nextId
  ): ExpressionDag[N] = new ExpressionDag[N] {
    def idToExp = id2Exp
    def roots = gcroots
    def nodeToLiteral = node2Literal
    def nextId = id
  }

  override def toString: String =
    s"ExpressionDag(idToExp = $idToExp)"

  // This is a cache of Id[T] => Option[N[T]]
  private val idToN =
    HCache.empty[Id, Lambda[t => Option[N[t]]]]
  private val nodeToId =
    HCache.empty[N, Lambda[t => Option[Id[t]]]]

  /**
   * Add a GC root, or tail in the DAG, that can never be deleted.
   */
  def addRoot[T](node: N[T]): (ExpressionDag[N], Id[T]) = {
    val (dag, id) = ensure(node)
    (dag.copy(gcroots = roots + id), id)
  }

  /**
   * Which ids are reachable from the roots
   */
  private def reachableIds: Set[Id[_]] = {
    // We actually don't care about the return type of the Set
    // This is a constant function at the type level
    type IdSet[t] = Set[Id[_]]
    def expand(s: Set[Id[_]]): Set[Id[_]] = {
      val f = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[Set[Id[_]]]]] {
        def apply[T] = {
          case (id, Expr.Const(_)) if s(id) => Some(s)
          case (id, Expr.Var(v)) if s(id) => Some(s + v)
          case (id, Expr.Unary(id0, _)) if s(id) => Some(s + id0)
          case (id, Expr.Binary(id0, id1, _)) if s(id) => Some((s + id0) + id1)
          case _ => None
        }
      }
      // Note this Stream must always be non-empty as long as roots are
      // TODO: we don't need to use collect here, just .get on each id in s
      idToExp.optionMap[IdSet](f).reduce(_ ++ _)
    }
    // call expand while we are still growing
    def go(s: Set[Id[_]]): Set[Id[_]] = {
      val step = expand(s)
      if (step == s) s else go(step)
    }
    go(roots)
  }

  private def gc: ExpressionDag[N] = {
    val goodIds = reachableIds
    val toKeepI2E = idToExp.filter(new FunctionK[HMap[Id, Expr[N, ?]]#Pair, BoolT] {
      def apply[T] = { case (id, _) => goodIds(id) }
    })
    copy(id2Exp = toKeepI2E)
  }

  /**
   * Apply the given rule to the given dag until
   * the graph no longer changes.
   */
  def apply(rule: Rule[N]): ExpressionDag[N] = {
    // for some reason, scala can't optimize this with tailrec
    var prev: ExpressionDag[N] = null
    var curr: ExpressionDag[N] = this
    while (!(curr eq prev)) {
      prev = curr
      curr = curr.applyOnce(rule)
    }
    curr
  }

  /**
   * Convert a N[T] to a Literal[T, N]
   */
  def toLiteral[T](n: N[T]): Literal[N, T] = nodeToLiteral.apply[T](n)

  /**
   * apply the rule at the first place that satisfies
   * it, and return from there.
   */
  def applyOnce(rule: Rule[N]): ExpressionDag[N] = {
    type DagT[T] = ExpressionDag[N]

    val f = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[DagT[x]]]] {
      def apply[U] = { (kv: (Id[U], Expr[N, U])) =>
        val (id, _) = kv
        val n1 = evaluate(id)
        rule.apply[U](self)(n1)
          .filter(_ != n1)
          .map { n2 =>
            val (dag, newId) = ensure(n2)

            // We can't delete Ids which may have been shared
            // publicly, and the ids may be embedded in many
            // nodes. Instead we remap 'id' to be a pointer
            // to 'newid'.
            dag.copy(id2Exp = dag.idToExp + (id -> Expr.Var[N, U](newId))).gc
          }
      }
    }

    idToExp.optionMap[DagT](f).headOption.getOrElse(this)
  }

  /**
   * This is only called by ensure
   *
   * Note, Expr must never be a Var
   */
  private def addExp[T](node: N[T], exp: Expr[N, T]): (ExpressionDag[N], Id[T]) = {
    require(!exp.isVar)

    find(node) match {
      case None =>
        val nodeId = Id[T](nextId)
        (copy(id2Exp = idToExp + (nodeId -> exp), id = nextId + 1), nodeId)
      case Some(id) =>
        (this, id)
    }
  }

  /**
   * This finds the Id[T] in the current graph that is equivalent
   * to the given N[T]
   */
  def find[T](node: N[T]): Option[Id[T]] =
    nodeToId.getOrElseUpdate(node, {
      val f = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[Id[x]]]] {
        // Make sure to return the original Id, not a Id -> Var -> Expr
        def apply[T1] = { case (thisId, expr) =>
          if (!expr.isVar && node == expr.evaluate(idToExp)) Some(thisId) else None
        }
      }

      idToExp.optionMap(f) match {
        case Stream.Empty =>
          None
        case id #:: Stream.Empty =>
          // this cast is safe if node == expr.evaluate(idToExp) implies types match
          Some(id).asInstanceOf[Option[Id[T]]]
        case _ =>
          // we'd like to make this an error; there should only ever
          // be zero or one ids for a node.
          None //sys.error(s"logic error, should only be one mapping: $node -> $others")
      }
    })

  /**
   * This throws if the node is missing, use find if this is not
   * a logic error in your programming. With dependent types we could
   * possibly get this to not compile if it could throw.
   */
  def idOf[T](node: N[T]): Id[T] =
    find(node).getOrElse {
      val msg = s"could not get node: $node\n from $this"
      throw new NoSuchElementException(msg)
    }

  /**
   * ensure the given literal node is present in the Dag
   * Note: it is important that at each moment, each node has
   * at most one id in the graph. Put another way, for all
   * Id[T] in the graph evaluate(id) is distinct.
   */
  protected def ensure[T](node: N[T]): (ExpressionDag[N], Id[T]) =
    find(node) match {
      case Some(id) => (this, id)
      case None =>
        toLiteral(node) match {
          case Literal.Const(n) =>
            /**
             * Since the code is not performance critical, but correctness critical, and we can't
             * check this property with the typesystem easily, check it here
             */
            require(n == node,
              s"Equality or nodeToLiteral is incorrect: nodeToLit($node) = Const($n)")
            addExp(node, Expr.Const(n))
          case Literal.Unary(prev, fn) =>
            val (exp1, idprev) = ensure(prev.evaluate)
            exp1.addExp(node, Expr.Unary(idprev, fn))
          case Literal.Binary(n1, n2, fn) =>
            val (exp1, id1) = ensure(n1.evaluate)
            val (exp2, id2) = exp1.ensure(n2.evaluate)
            exp2.addExp(node, Expr.Binary(id1, id2, fn))
        }
    }

  /**
   * After applying rules to your Dag, use this method
   * to get the original node type.
   * Only call this on an Id[T] that was generated by
   * this dag or a parent.
   */
  def evaluate[T](id: Id[T]): N[T] =
    evaluateOption(id).getOrElse {
      val msg = s"Could not evaluate: $id\nin $this"
      throw new NoSuchElementException(msg)
    }

  def evaluateOption[T](id: Id[T]): Option[N[T]] =
    idToN.getOrElseUpdate(id, {
      idToExp.get(id).map(_.evaluate(idToExp))
    })

  /**
   * Return the number of nodes that depend on the
   * given Id, TODO we might want to cache these.
   * We need to garbage collect nodes that are
   * no longer reachable from the root
   */
  def fanOut(id: Id[_]): Int =
    evaluateOption(id)
      .map(fanOut)
      .getOrElse(0)

  @annotation.tailrec
  private def dependsOn(expr: Expr[N, _], node: N[_]): Boolean = expr match {
    case Expr.Const(_) => false
    case Expr.Var(id) => dependsOn(idToExp(id), node)
    case Expr.Unary(id, _) => evaluate(id) == node
    case Expr.Binary(id0, id1, _) => evaluate(id0) == node || evaluate(id1) == node
  }

  /**
   * Returns 0 if the node is absent, which is true
   * use .contains(n) to check for containment
   */
  def fanOut(node: N[_]): Int = {
    val pointsToNode = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[N[x]]]] {
      def apply[T] = { case (id, expr) => if (dependsOn(expr, node)) Some(evaluate(id)) else None }
    }
    idToExp.optionMap(pointsToNode).toSet.size
  }
  def contains(node: N[_]): Boolean = find(node).isDefined
}

object ExpressionDag {

  private def empty[N[_]](n2l: FunctionK[N, Literal[N, ?]]): ExpressionDag[N] =
    new ExpressionDag[N] {
      val idToExp = HMap.empty[Id, Expr[N, ?]]
      val nodeToLiteral = n2l
      val roots = Set.empty[Id[_]]
      val nextId = 0
    }

  /**
   * This creates a new ExpressionDag rooted at the given tail node
   */
  def apply[T, N[_]](n: N[T], nodeToLit: FunctionK[N, Literal[N, ?]]): (ExpressionDag[N], Id[T]) =
    empty(nodeToLit).addRoot(n)

  /**
   * This is the most useful function. Given a N[T] and a way to convert to Literal[T, N],
   * apply the given rule until it no longer applies, and return the N[T] which is
   * equivalent under the given rule
   */
  def applyRule[T, N[_]](n: N[T],
    nodeToLit: FunctionK[N, Literal[N, ?]],
    rule: Rule[N]): N[T] = {
    val (dag, id) = apply(n, nodeToLit)
    dag(rule).evaluate(id)
  }
}

