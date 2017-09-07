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
 * Represents a directed acyclic graph (DAG).
 *
 * The type N[_] represents the type of nodes in the graph.
 */
sealed abstract class Dag[N[_]] { self =>

  /**
   * These have package visibility to test
   * the law that for all Expr, the node they
   * evaluate to is unique
   */
  protected def idToExp: HMap[Id, Expr[N, ?]]

  /**
   * The set of roots that were added by addRoot.
   * These are Ids that will always evaluate
   * such that roots.forall(evaluateOption(_).isDefined)
   */
  protected def roots: Set[Id[_]]

  /**
   * This is the next Id value which will be allocated
   */
  protected def nextId: Int

  /**
   * Convert a N[T] to a Literal[T, N].
   */
  def toLiteral: FunctionK[N, Literal[N, ?]]

  // Caches polymorphic functions of type T => Option[N[T]]
  private val idToN: HCache[Id, Lambda[t => Option[N[t]]]] =
    HCache.empty[Id, Lambda[t => Option[N[t]]]]

  // Caches polymorphic functions of type N[T] => Option[T]
  private val nodeToId: HCache[N, Lambda[t => Option[Id[t]]]] =
    HCache.empty[N, Lambda[t => Option[Id[t]]]]

  // Convenient method to produce new, modified DAGs based on this
  // one.
  private def copy(
      id2Exp: HMap[Id, Expr[N, ?]] = self.idToExp,
      node2Literal: FunctionK[N, Literal[N, ?]] = self.toLiteral,
      gcroots: Set[Id[_]] = self.roots,
      id: Int = self.nextId
  ): Dag[N] = new Dag[N] {
    def idToExp = id2Exp
    def roots = gcroots
    def toLiteral = node2Literal
    def nextId = id
  }

  // Produce a new DAG that is equivalent to this one, but which frees
  // orphaned nodes and other internal state which may no longer be
  // needed.
  private def gc: Dag[N] = {
    val keepers = reachableIds
    if (idToExp.forallKeys(keepers)) this
    else copy(id2Exp = idToExp.filterKeys(keepers))
  }

  /**
   * String representation of this DAG.
   */
  override def toString: String =
    s"Dag(idToExp = $idToExp, roots = $roots)"

  /**
   * Add a GC root, or tail in the DAG, that can never be deleted.
   */
  def addRoot[T](node: N[T]): (Dag[N], Id[T]) = {
    val (dag, id) = ensure(node)
    (dag.copy(gcroots = roots + id), id)
  }

  /**
   * Which ids are reachable from the roots?
   */
  def reachableIds: Set[Id[_]] = {

    def neighbors(i: Id[_]): List[Id[_]] =
      idToExp(i) match {
        case Expr.Const(_) => Nil
        case Expr.Var(id) => id :: Nil
        case Expr.Unary(id, _) => id :: Nil
        case Expr.Binary(id0, id1, _) => id0 :: id1 :: Nil
      }

    Graphs.reflexiveTransitiveClosure(roots.toList)(neighbors _).toSet
  }

  /**
   * Apply the given rule to the given dag until
   * the graph no longer changes.
   */
  def apply(rule: Rule[N]): Dag[N] = {

    @annotation.tailrec
    def loop(d: Dag[N]): Dag[N] = {
      val next = d.applyOnce(rule)
      if (next eq d) next
      else loop(next)
    }

    loop(this)
  }

  /**
   * Apply a sequence of rules, which you may think of as phases, in order
   * First apply one rule until it does not apply, then the next, etc..
   */
  def applySeq(phases: Seq[Rule[N]]): Dag[N] =
    phases.foldLeft(this) { (dag, rule) => dag(rule) }

  /**
   * apply the rule at the first place that satisfies
   * it, and return from there.
   */
  def applyOnce(rule: Rule[N]): Dag[N] = {
    type DagT[T] = Dag[N]

    val f = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[DagT[x]]]] {
      def toFunction[U] = { (kv: (Id[U], Expr[N, U])) =>
        val (id, expr) = kv

        if (expr.isVar) None // Vars always point somewhere, apply the rule there
        else {
          val n1 = evaluate(id)
          rule
            .apply[U](self)(n1)
            .filter(_ != n1)
            .map { n2 =>
              val (dag, newId) = ensure(n2)

              // We can't delete Ids which may have been shared
              // publicly, and the ids may be embedded in many
              // nodes. Instead we remap 'id' to be a pointer
              // to 'newid'.
              dag.copy(id2Exp = dag.idToExp.updated(id, Expr.Var[N, U](newId))).gc
            }
        }
      }
    }

    idToExp.optionMap[DagT](f).headOption.getOrElse(this)
  }

  /**
   * Apply a rule at most cnt times.
   */
  def applyMax(rule: Rule[N], cnt: Int): Dag[N] = {

    @annotation.tailrec
    def loop(d: Dag[N], cnt: Int): Dag[N] =
      if (cnt <= 0) d
      else {
        val next = d.applyOnce(rule)
        if (next eq d) d
        else loop(next, cnt - 1)
      }

    loop(this, cnt)
  }

  /**
   * This is only called by ensure
   *
   * Note, Expr must never be a Var
   */
  private def addExp[T](node: N[T], exp: Expr[N, T]): (Dag[N], Id[T]) = {
    require(!exp.isVar)
    val nodeId = Id[T](nextId)
    (copy(id2Exp = idToExp.updated(nodeId, exp), id = nextId + 1), nodeId)
  }

  /**
   * Find all the nodes currently in the graph
   */
  lazy val allNodes: Set[N[_]] = {
    type Node = Either[Id[_], Expr[N, _]]
    def deps(n: Node): List[Node] = n match {
      case Right(Expr.Const(_)) => Nil
      case Right(Expr.Var(id)) => Left(id) :: Nil
      case Right(Expr.Unary(id, _)) => Left(id) :: Nil
      case Right(Expr.Binary(id0, id1, _)) => Left(id0) :: Left(id1) :: Nil
      case Left(id) => idToExp.get(id).map(Right(_): Node).toList
    }
    val all = Graphs.reflexiveTransitiveClosure(roots.toList.map(Left(_): Node))(deps _)

    val evalMemo = Expr.evaluateMemo(idToExp)
    all.iterator.collect { case Right(expr) => evalMemo(expr) }.toSet
  }

  /**
   * This finds an Id[T] in the current graph that is equivalent
   * to the given N[T]
   */
  def find[T](node: N[T]): Option[Id[T]] =
    nodeToId.getOrElseUpdate(
      node, {
        findAll(node).filterNot { id => idToExp(id).isVar } match {
          case Stream.Empty =>
            // if the node is the in the graph it has at least
            // one non-Var node
            None
          case nonEmpty =>
            // there can be duplicate ids. Consider this case:
            // Id(0) -> Expr.Unary(Id(1), fn)
            // Id(1) -> Expr.Const(n1)
            // Id(2) -> Expr.Unary(Id(3), fn)
            // Id(3) -> Expr.Const(n2)
            //
            // then, a rule replaces n1 and n2 both with n3 Then, we'd have
            // Id(1) -> Var(Id(4))
            // Id(4) -> Expr.Const(n3)
            // Id(3) -> Var(Id(4))
            //
            // and now, Id(0) and Id(2) both point to non-Var nodes, but also
            // both are equal

            // Prefer to return a root Id, if there is one
            val matchingRoots = nonEmpty.filter(roots)
            if (matchingRoots.isEmpty) Some(nonEmpty.min)
            else Some(matchingRoots.min)
        }
      }
    )

  /**
   * Nodes can have multiple ids in the graph, this gives all of them
   */
  def findAll[T](node: N[T]): Stream[Id[T]] = {
    /*
     * This method looks through linearly evaluating nodes
     * until we find the given node. Keep a cache of
     * Expr -> N open for the entire call
     */
    val evalExpr = Expr.evaluateMemo(idToExp)

    val f = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[Id[x]]]] {
      def toFunction[T1] = {
        case (thisId, expr) =>
          if (node == evalExpr(expr)) Some(thisId) else None
      }
    }

    // this cast is safe if node == expr.evaluate(idToExp) implies types match
    idToExp.optionMap(f).asInstanceOf[Stream[Id[T]]]
  }


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
  protected def ensure[T](node: N[T]): (Dag[N], Id[T]) =
    find(node) match {
      case Some(id) => (this, id)
      case None =>
        toLiteral(node) match {
          case Literal.Const(n) =>
            /*
             * Since the code is not performance critical, but correctness critical, and we can't
             * check this property with the typesystem easily, check it here
             */
            require(n == node,
                    s"Equality or toLiteral is incorrect: nodeToLit($node) = Const($n)")
            addExp(node, Expr.Const(n))
          case Literal.Unary(prev, fn) =>
            val (exp1, idprev) = ensure(prev.evaluate)
            exp1.addExp(node, Expr.Unary(idprev, fn))
          case Literal.Binary(n1, n2, fn) =>
            // use a common memoized function on both branches
            val evalLit = Literal.evaluateMemo[N]
            val (exp1, id1) = ensure(evalLit(n1))
            val (exp2, id2) = exp1.ensure(evalLit(n2))
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

  /**
   * Returns 0 if the node is absent, which is true
   * use .contains(n) to check for containment
   */
  def fanOut(node: N[_]): Int = {
    val interiorFanOut = dependentsOf(node).size
    val tailFanOut = if (isRoot(node)) 1 else 0

    interiorFanOut + tailFanOut
  }

  /**
   * Is this node a root of this graph
   */
  def isRoot(n: N[_]): Boolean =
    findAll(n).exists(roots)

  /**
   * Is this node in this DAG
   */
  def contains(node: N[_]): Boolean =
    find(node).isDefined

  /**
   * list all the nodes that depend on the given node
   */
  def dependentsOf(node: N[_]): Set[N[_]] = {

    def dependsOn(expr: Expr[N, _]): Boolean = expr match {
      case Expr.Const(_) => false
      case Expr.Var(id) => sys.error(s"logic error: Var($id)")
      case Expr.Unary(id, _) => evaluate(id) == node
      case Expr.Binary(id0, id1, _) => evaluate(id0) == node || evaluate(id1) == node
    }

    // TODO, we can do a much better algorithm that builds this function
    // for all nodes in the dag
    val pointsToNode = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[N[x]]]] {
      def toFunction[T] = {
        case (id, expr) =>
          // We can ignore Vars here, since all vars point to a final expression
          if ((!expr.isVar) && dependsOn(expr)) Some(evaluate(id))
          else None
      }
    }
    idToExp.optionMap(pointsToNode).toSet
  }

  /**
   * Return all dependendants of a given node.
   * Does not include itself
   */
  def transitiveDependentsOf(p: N[_]): Set[N[_]] = {
    def nfn(n: N[Any]): List[N[Any]] =
      dependentsOf(n).toList.asInstanceOf[List[N[Any]]]

    Graphs.depthFirstOf(p.asInstanceOf[N[Any]])(nfn _).toSet
  }
}

object Dag {

  def empty[N[_]](n2l: FunctionK[N, Literal[N, ?]]): Dag[N] =
    new Dag[N] {
      val idToExp = HMap.empty[Id, Expr[N, ?]]
      val toLiteral = n2l
      val roots = Set.empty[Id[_]]
      val nextId = 0
    }

  /**
   * This creates a new Dag rooted at the given tail node
   */
  def apply[T, N[_]](n: N[T], nodeToLit: FunctionK[N, Literal[N, ?]]): (Dag[N], Id[T]) =
    empty(nodeToLit).addRoot(n)

  /**
   * This is the most useful function. Given a N[T] and a way to convert to Literal[T, N],
   * apply the given rule until it no longer applies, and return the N[T] which is
   * equivalent under the given rule
   */
  def applyRule[T, N[_]](n: N[T], nodeToLit: FunctionK[N, Literal[N, ?]], rule: Rule[N]): N[T] = {
    val (dag, id) = apply(n, nodeToLit)
    dag(rule).evaluate(id)
  }
}
