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

import java.io.Serializable
import scala.util.control.TailCalls
import scala.collection.immutable.SortedMap
/**
 * Represents a directed acyclic graph (DAG).
 *
 * The type N[_] represents the type of nodes in the graph.
 */
sealed abstract class Dag[N[_]] extends Serializable { self =>

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
   * Convert a N[T] to a Literal[T, N].
   */
  def toLiteral: FunctionK[N, Literal[N, ?]]

  // Caches polymorphic functions of type Id[T] => Option[N[T]]
  private val idToN: HCache[Id, Lambda[t => Option[N[t]]]] =
    HCache.empty[Id, Lambda[t => Option[N[t]]]]

  // Caches polymorphic functions of type Literal[N, T] => Option[Id[T]]
  private val litToId: HCache[Literal[N, ?], Lambda[t => Option[Id[t]]]] =
    HCache.empty[Literal[N, ?], Lambda[t => Option[Id[t]]]]

  // Caches polymorphic functions of type Expr[N, T] => N[T]
  private val evalMemo = Expr.evaluateMemo(idToExp)


  // Which id nodes depend on a given id
  protected def idDepGraph: SortedMap[Id[_], Set[Id[_]]]

  /**
   * String representation of this DAG.
   */
  override def toString: String =
    s"Dag(idToExp = $idToExp, roots = $roots)"

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
              val newIdN1 = Id.next[U]()
              val dag1 = replaceId(newIdN1, expr, n1)
              val (dag2, newId) = dag1.ensure(n2)

              // We can't delete Ids which may have been shared
              // publicly, and the ids may be embedded in many
              // nodes. Instead we remap 'ids' to be a pointer
              // to 'newid'.
              dag2.repointIds(n1, oldIds, idDepGraph, newId, n2)
            }
        }
      }
    }

    // We want to apply rules
    // in a deterministic order so they are reproducible
    // idDepGraph is sorted by Id, so it is consistently ordered
    idDepGraph
      .iterator
      .map { case (id, _) =>
        // use the method to fix the types below
        // if we don't use DagT here, scala thinks
        // it is unused even though we use it above
        def go[A](id: Id[A]): Option[DagT[A]] = {
          val expr = idToExp(id)
          f.toFunction[A]((id, expr))
        }
        go(id)
      }
      .collectFirst { case Some(dag) => dag }
      .getOrElse(this)
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

  ////////////////////////////
  //
  //  These following methods are the only methods that directly
  //  allocate new Dag instances. These are where all invariants
  //  must be maintained
  //
  ////////////////////////////

  /**
   * Add a GC root, or tail in the DAG, that can never be deleted.
   */
  def addRoot[T](node: N[T]): (Dag[N], Id[T]) = {
    val (dag, id) = ensure(node)
    (dag.copy(gcroots = dag.roots + id), id)
  }

  // Convenient method to produce new, modified DAGs based on this
  // one.
  private def copy(
      id2Exp: HMap[Id, Expr[N, ?]] = self.idToExp,
      node2Literal: FunctionK[N, Literal[N, ?]] = self.toLiteral,
      gcroots: Set[Id[_]] = self.roots,
      idDeps: SortedMap[Id[_], Set[Id[_]]] = idDepGraph
  ): Dag[N] = new Dag[N] {
    def idToExp = id2Exp
    def roots = gcroots
    def toLiteral = node2Literal
    def idDepGraph = idDeps
  }

  // Produce a new DAG that is equivalent to this one, but which frees
  // orphaned nodes and other internal state which may no longer be
  // needed.
  private def gc: Dag[N] = {
    val keepers = reachableIds
    if (idToExp.forallKeys(keepers)) this
    else copy(
      id2Exp = idToExp.filterKeys(keepers),
      idDeps = idDepGraph.filterKeys(keepers))
  }

  /*
   * This updates the canonical Id for a given node and expression
   */
  protected def replaceId[A](newId: Id[A], expr: Expr[N, A], node: N[A]): Dag[N] =
    copy(
      id2Exp = idToExp.updated(newId, expr),
      idDeps = idDepGraph.updated(newId, Set.empty))

  protected def repointIds[A](
    orig: N[A],
    oldIds: Iterable[Id[A]],
    initChildren: Map[Id[_], Set[Id[_]]],
    newId: Id[A],
    newNode: N[A]): Dag[N] =
    if (oldIds.nonEmpty) {
      val idExp1 = oldIds.foldLeft(idToExp) { (mapping, origId) =>
        val m1 = mapping.updated(origId, Expr.Var[N, A](newId))
        initChildren.get(origId) match {
          case None => m1
          case Some(children) =>
            children.foldLeft(m1) { (m2, id) =>
              def go[B](id: Id[B]): HMap[Id, Expr[N, ?]] = {
                m2.get(id) match {
                  case None => m1
                  case Some(expr) =>
                    val newExpr = Expr.repoint[N, B, A](expr, origId, newId)
                    m2.updated(id, newExpr)
                }
              }
              go(id)
            }
        }
      }
      // now update the idDepGraph, all the children of the oldIds, point to the newId
      val oldChildren = oldIds.foldLeft(idDepGraph.getOrElse(newId, Set.empty)) { (down, id) =>
        initChildren.get(id) match {
          case None => down
          case Some(s) => s.asInstanceOf[Set[Id[Any]]] ++ down
        }
      }
      val idDep1 = idDepGraph.updated(newId, oldChildren)
      // now remove all those children from each oldId
      val idDep2 = oldIds.foldLeft(idDep1) { (idDepGraph, oldId) =>
        idDepGraph.get(oldId) match {
          case None => idDepGraph
          case Some(g) =>
            val toRem = initChildren.getOrElse(oldId, Set.empty[Id[_]])
            val newg = g -- toRem
            // we need to keep all the IDs in here, we can't remove empty sets
            idDepGraph.updated(oldId, newg)
        }
      }
      copy(
        id2Exp = idExp1,
        idDeps = idDep2
        ).gc
    }
    else this

  @annotation.tailrec
  private def dependsOnIds[A](e: Expr[N, A]): List[Id[_]] =
    e match {
      case Expr.Var(id) => dependsOnIds(idToExp(id))
      case _ => Expr.dependsOnIds(e)
    }

  /**
   * This is only called by ensure
   *
   * Note, Expr must never be a Var
   */
  private def addExp[T](exp: Expr[N, T]): (Dag[N], Id[T]) = {
    require(!exp.isVar)
    val nodeId = Id.next[T]()
    val deps = dependsOnIds(exp)
    val newIdDep = deps.foldLeft(idDepGraph) { (dg, to) =>
      dg.get(to) match {
        case None => dg.updated(to, Set(nodeId).asInstanceOf[Set[Id[_]]])
        case Some(s) => dg.updated(to, s + nodeId)
      }
    }
    (copy(
      id2Exp = idToExp.updated(nodeId, exp),
      idDeps = newIdDep.updated(nodeId, Set.empty)
      ), nodeId)
  }

  ////////////////////////////
  //
  // End of methods that direcly allocate new Dag instances
  //
  ////////////////////////////

  /**
   * This finds an Id[T] in the current graph that is equivalent
   * to the given N[T]
   */
  def find[T](node: N[T]): Option[Id[T]] =
    findLiteral(toLiteral(node), node)

  private def findLiteral[T](lit: Literal[N, T], n: => N[T]): Option[Id[T]] =
    litToId.getOrElseUpdate(
      lit, {
        // It's important to not compare equality in the Literal
        // space because it can have function members that are
        // equivalent, but not .equals
        findAll(n).filterNot { id => idToExp(id).isVar } match {
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
    // TODO: this computation is really expensive, 60% of CPU in a recent benchmark
    // maintaining these mappings would be nice, but maybe expensive as we are rewriting
    // nodes
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
   * ensure the given literal node is present in the Dag
   * Note: it is important that at each moment, each node has
   * at most one id in the graph. Put another way, for all
   * Id[T] in the graph evaluate(id) is distinct.
   */
  protected def ensure[T](node: N[T]): (Dag[N], Id[T]) = {
    val lit = toLiteral(node)
    try ensureFast(lit)
    catch {
      case _: StackOverflowError =>
        ensureRec(lit).result
    }
  }

  /*
   * This does recursion on the stack, which is faster, but can overflow
   */
  protected def ensureFast[T](lit: Literal[N, T]): (Dag[N], Id[T]) =
    findLiteral(lit, lit.evaluate) match {
      case Some(id) => (this, id)
      case None =>
        lit match {
          case Literal.Const(n) =>
            addExp(Expr.Const(n))
          case Literal.Unary(prev, fn) =>
            val (exp1, idprev) = ensureFast(prev)
            exp1.addExp(Expr.Unary(idprev, fn))
          case Literal.Binary(n1, n2, fn) =>
            val (exp1, id1) = ensureFast(n1)
            val (exp2, id2) = exp1.ensureFast(n2)
            exp2.addExp(Expr.Binary(id1, id2, fn))
          case Literal.Variadic(args, fn) =>
            @annotation.tailrec
            def go[A](dag: Dag[N], args: List[Literal[N, A]], acc: List[Id[A]]): (Dag[N], List[Id[A]]) =
              args match {
                case Nil => (dag, acc.reverse)
                case h :: tail =>
                   val (dag1, hid) = dag.ensureFast(h)
                   go(dag1,tail, hid :: acc)
              }

            val (d, ids) = go(this, args, Nil)
            d.addExp(Expr.Variadic(ids, fn))
        }
    }

  protected def ensureRec[T](lit: Literal[N, T]): TailCalls.TailRec[(Dag[N], Id[T])] =
    findLiteral(lit, lit.evaluate) match {
      case Some(id) => TailCalls.done((this, id))
      case None =>
        lit match {
          case Literal.Const(n) =>
            TailCalls.done(addExp(Expr.Const(n)))
          case Literal.Unary(prev, fn) =>
            TailCalls.tailcall(ensureRec(prev)).map { case (exp1, idprev) =>
              exp1.addExp(Expr.Unary(idprev, fn))
            }
          case Literal.Binary(n1, n2, fn) =>
            for {
              p1 <- TailCalls.tailcall(ensureRec(n1))
              (exp1, id1) = p1
              p2 <- TailCalls.tailcall(exp1.ensureRec(n2))
              (exp2, id2) = p2
            } yield exp2.addExp(Expr.Binary(id1, id2, fn))
          case Literal.Variadic(args, fn) =>
            def go[A](dag: Dag[N], args: List[Literal[N, A]]): TailCalls.TailRec[(Dag[N], List[Id[A]])] =
              args match {
                case Nil => TailCalls.done((dag, Nil))
                case h :: tail =>
                  for {
                    rest <- go(dag, tail)
                    (dag1, its) = rest
                    dagH <- TailCalls.tailcall(dag1.ensureRec(h))
                    (dag2, idh) = dagH
                  } yield (dag2, idh :: its)
              }

            go(this, args).map { case (d, ids)  =>
              d.addExp(Expr.Variadic(ids, fn))
            }
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
  private def fanOut(ids: Stream[Id[_]]): Int = {
    val idSet = ids.toSet
    val interiorFanNodes = idSet.flatMap(dependentsOfId(_)).map(evaluate(_))

    val interiorFanOut = interiorFanNodes.size
    val isroot = (roots & idSet).nonEmpty || ids.exists { i => isRoot(evaluate(i)) }
    val tailFanOut = if (isroot) 1 else 0

    interiorFanOut + tailFanOut
  }

  def fanOut(id: Id[_]): Int = fanOut(id #:: Stream.empty)

  /**
   * Returns 0 if the node is absent, which is true
   * use .contains(n) to check for containment
   */
  def fanOut(node: N[_]): Int =
    fanOut(findAll(node))

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
   * list all the nodes that depend on the given node
   */
  def dependentsOf(node: N[_]): Set[N[_]] =
    find(node) match {
      case None => Set.empty
      case Some(id) =>
        dependentsOfId(id).iterator.flatMap { depId =>
          evaluateOption(depId) match {
            case None => Set.empty[N[_]]
            case Some(n) => Set(n): Set[N[_]]
          }
        }
        .toSet
    }

  private def dependentsOfId(id: Id[_]): Set[Id[_]] =
    idDepGraph.get(id) match {
      case None => Set.empty
      case Some(deps) =>
        // how many distinct nodes are here:
        def pointsToId(i: Id[_]): Boolean =
          idToExp.get(i) match {
            case None => false
            case Some(expr) =>
              require(id != i, s"id: $id depends on itself")
              dependsOnIds(expr).contains(id)
          }

        deps.filter(pointsToId _)
    }

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
      // Using the reverse order here is important since we tend to expose the deepest
      // nodes in the graph first. This allows rules to do more work internally and be
      // MUCH faster if they can recursively apply themselves before making a modification
      // to the graph
      val idDepGraph = SortedMap.empty[Id[Any], Set[Id[_]]](Id.idOrdering[Any].reverse)
        .asInstanceOf[SortedMap[Id[_], Set[Id[_]]]]
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
