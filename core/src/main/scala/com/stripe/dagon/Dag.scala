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

  // Caches polymorphic functions of type Id[T] => Option[N[T]]
  private val idToN: HCache[Id, Lambda[t => Option[N[t]]]] =
    HCache.empty[Id, Lambda[t => Option[N[t]]]]

  // Caches polymorphic functions of type N[T] => Option[Id[T]]
  private val nodeToId: HCache[N, Lambda[t => Option[Id[t]]]] =
    HCache.empty[N, Lambda[t => Option[Id[t]]]]

  // Caches polymorphic functions of type Expr[N, T] => N[T]
  private val evalMemo = Expr.evaluateMemo(idToExp)

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
    else {
      val id2Exp = idToExp.filterKeys(keepers)
      // if we have GC'd an id it is because
      // it did not escape. I.e. it is not a root,
      // nor reachable from the root, so we can
      // possibly lower the nextId, which is a bit
      // nice because (unfortunately) Ids wrap Int
      // not Long, so overflow is possible
      val nextId = id2Exp.keySet.iterator.map(_.id).max
      copy(id2Exp = id2Exp, id = nextId + 1)
    }
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
    (dag.copy(gcroots = dag.roots + id), id)
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
        case Expr.Variadic(ids, _) => ids
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
              // A node can have several Ids.
              // we need to point ALL of the old ids to the new one
              val oldIds =
                findAll(n1) match {
                  case absent if absent.isEmpty =>
                    sys.error(s"unreachable code, $n1 should have id $id")
                  case existing => existing
                }
              // If n2 depends on n1, the Var trick fails and introduces
              // loops. To avoid this, we have to work in an edge based
              // approach. For all n3 != n2, if they depend on n1, replace
              // with n2. Leave n2 alone.

              // Get an ID for the new node
              // if this new node points to the old node
              // we are going to create a cycle, since
              // below we point the old nodes back to the
              // new id. To fix this, re-reassign
              // n1 to a new id, since that new id won't be
              // updated to point to itself, we prevent a loop
              val dag1 = copy(id2Exp = idToExp.updated(Id(nextId), expr), id = nextId + 1)
              val (dag2, newId) = dag1.ensure(n2)

              // We can't delete Ids which may have been shared
              // publicly, and the ids may be embedded in many
              // nodes. Instead we remap 'ids' to be a pointer
              // to 'newid'.
              val newIdToExp = oldIds.foldLeft(dag2.idToExp) { (mapping, origId) =>
                mapping.updated(origId, Expr.Var[N, U](newId))
              }
              dag2.copy(id2Exp = newIdToExp).gc
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

  def depthOfId[A](i: Id[A]): Option[Int] =
    depth(i)

  def depthOf[A](n: N[A]): Option[Int] =
    find(n).flatMap(depthOfId(_))

  private val depth: Id[_] => Option[Int] =
    Memoize.function[Id[_], Option[Int]] { (id, rec) =>
      idToExp.get(id) match {
        case None => None
        case Some(Expr.Const(_)) => Some(0)
        case Some(Expr.Var(id)) => rec(id)
        case Some(Expr.Unary(id, _)) => rec(id).map(_ + 1)
        case Some(Expr.Binary(id0, id1, _)) =>
          for {
            d0 <- rec(id0)
            d1 <- rec(id1)
          } yield math.max(d0, d1) + 1
        case Some(Expr.Variadic(ids, _)) =>
          ids.foldLeft(Option(0)) { (optD, id) =>
            for {
              d <- optD
              d1 <- rec(id)
            } yield math.max(d, d1) + 1
          }
      }
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
      case Right(Expr.Variadic(ids, _)) => ids.map(Left(_))
      case Left(id) => idToExp.get(id).map(Right(_): Node).toList
    }
    val all = Graphs.reflexiveTransitiveClosure(roots.toList.map(Left(_): Node))(deps _)

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

            // We use the maximum ID which is important to deal with
            // cycle avoidance in applyRule since we guarantee
            // that all the nodes that are repointed are computed
            // before we add a new node to graph
            Some(nonEmpty.max)
        }
      }
    )

  /**
   * Nodes can have multiple ids in the graph, this gives all of them
   */
  def findAll[T](node: N[T]): Stream[Id[T]] = {

    val f = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[Id[x]]]] {
      def toFunction[T1] = {
        case (thisId, expr) =>
          if (node == evalMemo(expr)) Some(thisId) else None
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
            // this is safe because Literal does not know about Id
            val evalLit = Literal.evaluateMemo[N]
            val (exp1, id1) = ensure(evalLit(n1))
            val (exp2, id2) = exp1.ensure(evalLit(n2))
            exp2.addExp(node, Expr.Binary(id1, id2, fn))
          case Literal.Variadic(args, fn) =>
            val evalLit = Literal.evaluateMemo[N]
            val init: Dag[N] = this
            def go[A](args: List[Literal[N, A]]): (Dag[N], List[Id[A]]) = {
              val (e, ids) = args.foldLeft((init, List.empty[Id[A]])) { case ((exp, ids), n) =>
                val (nextExp, id) = exp.ensure(evalLit(n))
                (nextExp, id :: ids)
              }
              (e, ids.reverse)
            }
            val (exp1, ids) = go(args)
            exp1.addExp(node, Expr.Variadic(ids, fn))
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
      idToExp.get(id).map(evalMemo(_))
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
    roots.iterator.exists(evaluatesTo(_, n))

  /**
   * Is this node in this DAG
   */
  def contains(node: N[_]): Boolean =
    find(node).isDefined

  /**
   * What nodes do we depend directly on
   */
  def dependenciesOf(node: N[_]): List[N[_]] = {
    toLiteral(node) match {
      case Literal.Const(_) =>
        Nil
      case Literal.Unary(n, _) =>
        n.evaluate :: Nil
      case Literal.Binary(n1, n2, _) =>
        val evalLit = Literal.evaluateMemo[N]
        evalLit(n1) :: evalLit(n2) :: Nil
      case Literal.Variadic(inputs, _) =>
        val evalLit = Literal.evaluateMemo[N]
        inputs.map(evalLit(_))
    }
  }

  /**
   * It is as expensive to compute this for the whole graph
   * as it is to answer a single query
   * we already cache the N pointed to, so this structure
   * should be small
   */
  private lazy val dependencyMap: Map[N[_], Set[N[_]]] = {
    def dependsOnSet(expr: Expr[N, _]): Set[N[_]] = expr match {
      case Expr.Const(_) => Set.empty
      case Expr.Var(id) => sys.error(s"logic error: Var($id)")
      case Expr.Unary(id, _) => Set(evaluate(id))
      case Expr.Binary(id0, id1, _) => Set(evaluate(id0), evaluate(id1))
      case Expr.Variadic(ids, _) => ids.iterator.map(evaluate(_)).toSet
    }

    type SetConst[T] = (N[T], Set[N[_]])
    val pointsToNode = new FunctionK[HMap[Id, Expr[N, ?]]#Pair, Lambda[x => Option[SetConst[x]]]] {
      def toFunction[T] = {
        case (id, expr) =>
          // here are the nodes we depend on:

          // We can ignore Vars here, since all vars point to a final expression
          if (!expr.isVar) {
            val depSet = dependsOnSet(expr)
            Some((evalMemo(expr), depSet))
          }
          else None
      }
    }

    idToExp.optionMap[SetConst](pointsToNode)
      .flatMap { case (n, deps) =>
        deps.map((_, n): (N[_], N[_]))
      }
      .groupBy(_._1)
      .iterator
      .map { case (k, vs) => (k, vs.iterator.map(_._2).toSet) }
      .toMap
  }

  /**
   * list all the nodes that depend on the given node
   */
  def dependentsOf(node: N[_]): Set[N[_]] =
    dependencyMap.getOrElse(node, Set.empty)

  private def evaluatesTo[A, B](id: Id[A], n: N[B]): Boolean = {
    val idN = evaluate(id)
    // since we cache, reference equality will often work
    val refEq = idN.asInstanceOf[AnyRef] eq id.asInstanceOf[AnyRef]
    refEq || (idN == n)
  }

  /**
   * equivalent to (but maybe faster than) fanOut(n) <= 1
   */
  def hasSingleDependent(n: N[_]): Boolean =
    fanOut(n) <= 1

  /**
   * Return all dependents of a given node.
   * Does not include itself
   */
  def transitiveDependentsOf(p: N[_]): Set[N[_]] = {
    def nfn(n: N[Any]): List[N[Any]] =
      dependentsOf(n).toList.asInstanceOf[List[N[Any]]]

    Graphs.depthFirstOf(p.asInstanceOf[N[Any]])(nfn _).toSet
  }

  /**
   * Return the transitive dependencies of a given node
   */
  def transitiveDependenciesOf(p: N[_]): Set[N[_]] = {
    def nfn(n: N[Any]): List[N[Any]] =
      dependenciesOf(n).toList.asInstanceOf[List[N[Any]]]

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
