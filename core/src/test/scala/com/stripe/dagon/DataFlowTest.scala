package com.stripe.dagon

import org.scalatest.FunSuite
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks._

object DataFlowTest {
  sealed trait Flow[+T] {
    def filter(fn: T => Boolean): Flow[T] =
      optionMap(Flow.FilterFn(fn))

    def map[U](fn: T => U): Flow[U] =
      optionMap(Flow.MapFn(fn))

    def optionMap[U](fn: T => Option[U]): Flow[U] =
      Flow.OptionMapped(this, fn)

    def concatMap[U](fn: T => TraversableOnce[U]): Flow[U] =
      Flow.ConcatMapped(this, fn)

    def ++[U >: T](that: Flow[U]): Flow[U] =
      Flow.Merge(this, that)

    def tagged[A](a: A): Flow[T] =
      Flow.Tagged(this, a)
  }

  object Flow {
    def apply[T](it: Iterator[T]): Flow[T] = IteratorSource(it)

    def dependenciesOf(f: Flow[Any]): List[Flow[Any]] =
      f match {
        case IteratorSource(_) => Nil
        case OptionMapped(f, _) => f :: Nil
        case ConcatMapped(f, _) => f :: Nil
        case Tagged(f, _) => f :: Nil
        case Fork(f) => f :: Nil
        case Merge(left, right) => left :: right :: Nil
        case Merged(ins) => ins
      }

    def transitiveDeps(f: Flow[Any]): List[Flow[Any]] =
      Graphs.reflexiveTransitiveClosure(List(f))(dependenciesOf _)

    case class IteratorSource[T](it: Iterator[T]) extends Flow[T]
    case class OptionMapped[T, U](input: Flow[T], fn: T => Option[U]) extends Flow[U]
    case class ConcatMapped[T, U](input: Flow[T], fn: T => TraversableOnce[U]) extends Flow[U]
    case class Merge[T](left: Flow[T], right: Flow[T]) extends Flow[T]
    case class Merged[T](inputs: List[Flow[T]]) extends Flow[T]
    case class Tagged[A, T](input: Flow[T], tag: A) extends Flow[T]
    case class Fork[T](input: Flow[T]) extends Flow[T]

    def toLiteral: FunctionK[Flow, Literal[Flow, ?]] =
      Memoize.functionK[Flow, Literal[Flow, ?]](new Memoize.RecursiveK[Flow, Literal[Flow, ?]] {
        import Literal._

        def toFunction[T] = {
          case (it@IteratorSource(_), _) => Const(it)
          case (o: OptionMapped[s, T], rec) => Unary(rec[s](o.input), { f: Flow[s] => OptionMapped(f, o.fn) })
          case (c: ConcatMapped[s, T], rec) => Unary(rec[s](c.input), { f: Flow[s] => ConcatMapped(f, c.fn) })
          case (t: Tagged[a, s], rec) => Unary(rec[s](t.input), { f: Flow[s] => Tagged(f, t.tag) })
          case (f: Fork[s], rec) => Unary(rec[s](f.input), { f: Flow[s] => Fork(f) })
          case (m: Merge[s], rec) => Binary(rec(m.left), rec(m.right), { (l: Flow[s], r: Flow[s]) => Merge(l, r) })
          case (m: Merged[s], rec) => Variadic(m.inputs.map(rec(_)), { fs: List[Flow[s]] => Merged(fs) })
        }
      })

    /*
     * use case class functions to preserve equality where possible
     */
    private case class FilterFn[A](fn: A => Boolean) extends Function1[A, Option[A]] {
      def apply(a: A): Option[A] = if (fn(a)) Some(a) else None
    }

    private case class MapFn[A, B](fn: A => B) extends Function1[A, Option[B]] {
      def apply(a: A): Option[B] = Some(fn(a))
    }

    private case class ComposedOM[A, B, C](fn1: A => Option[B], fn2: B => Option[C]) extends Function1[A, Option[C]] {
      def apply(a: A): Option[C] = fn1(a).flatMap(fn2)
    }
    private case class ComposedCM[A, B, C](fn1: A => TraversableOnce[B], fn2: B => TraversableOnce[C]) extends Function1[A, TraversableOnce[C]] {
      def apply(a: A): TraversableOnce[C] = fn1(a).flatMap(fn2)
    }
    private case class OptionToConcatFn[A, B](fn: A => Option[B]) extends Function1[A, TraversableOnce[B]] {
      def apply(a: A): TraversableOnce[B] = fn(a) match {
        case Some(a) => Iterator.single(a)
        case None => Iterator.empty
      }
    }

    /**
     * Add explicit fork
     * this is useful if you don't want to have to check each rule for
     * fanout
     */
    object explicitFork extends Rule[Flow] {
      def apply[T](on: Dag[Flow]) = {
        case Fork(_) => None
        case flow if on.fanOut(flow) > 1 => Some(Fork(flow))
        case _ => None
      }
    }
    /**
     * f.optionMap(fn1).optionMap(fn2) == f.optionMap { t => fn1(t).flatMap(fn2) }
     * we use object to get good toString for debugging
     */
    object composeOptionMapped extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case (OptionMapped(inner @ OptionMapped(s, fn0), fn1)) if on.fanOut(inner) == 1 =>
          OptionMapped(s, ComposedOM(fn0, fn1))
      }
    }

    /**
     * f.concatMap(fn1).concatMap(fn2) == f.concatMap { t => fn1(t).flatMap(fn2) }
     */
    object composeConcatMap extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case (ConcatMapped(inner @ ConcatMapped(s, fn0), fn1)) if on.fanOut(inner) == 1 =>
          ConcatMapped(s, ComposedCM(fn0, fn1))
      }
    }

    /**
     * (a ++ b).concatMap(fn) == (a.concatMap(fn) ++ b.concatMap(fn))
     * (a ++ b).optionMap(fn) == (a.optionMap(fn) ++ b.optionMap(fn))
     */
    object mergePullDown extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case (ConcatMapped(merge @ Merge(a, b), fn)) if on.fanOut(merge) == 1 =>
          a.concatMap(fn) ++ b.concatMap(fn)
        case (OptionMapped(merge @ Merge(a, b), fn)) if on.fanOut(merge) == 1 =>
          a.optionMap(fn) ++ b.optionMap(fn)
      }
    }

    /**
     * we can convert optionMap to concatMap if we don't care about maintaining
     * the knowledge about which fns potentially expand the size
     */
    object optionMapToConcatMap extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case OptionMapped(of, fn) => ConcatMapped(of, OptionToConcatFn(fn))
      }
    }

    /**
     * right associate merges
     */
    object CombineMerges extends Rule[Flow] {
      def apply[T](on: Dag[Flow]) = {
        @annotation.tailrec
        def flatten(f: Flow[T], toCheck: List[Flow[T]], acc: List[Flow[T]]): List[Flow[T]] =
          f match {
            case m@Merge(a, b) if on.fanOut(m) == 1 =>
              // on the inner merges, we only destroy them if they have no fanout
              flatten(a, b :: toCheck, acc)
            case noSplit =>
              toCheck match {
                case h :: tail => flatten(h, tail, noSplit :: acc)
                case Nil => (noSplit :: acc).reverse
            }
          }

        { node: Flow[T] =>

          node match {
            case Merge(a, b) =>
              flatten(a, b :: Nil, Nil) match {
                case a1 :: a2 :: Nil =>
                  None // could not simplify
                case many => Some(Merged(many))
              }
            case Merged(list@(h :: tail)) =>
              val res = flatten(h, tail, Nil)
              if (res != list) Some(Merged(res))
              else None
            case _ => None
          }
        }
      }
    }

    /**
     *  evaluate single fanout sources
     */
    object evalSource extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case OptionMapped(src @ IteratorSource(it), fn) if on.fanOut(src) == 1 =>
          IteratorSource(it.flatMap(fn(_).toIterator))
        case ConcatMapped(src @ IteratorSource(it), fn) if on.fanOut(src) == 1 =>
          IteratorSource(it.flatMap(fn))
        case Merge(src1 @ IteratorSource(it1), src2 @ IteratorSource(it2)) if it1 != it2 && on.fanOut(src1) == 1 && on.fanOut(src2) == 1 =>
          IteratorSource(it1 ++ it2)
        case Merge(src1 @ IteratorSource(it1), src2 @ IteratorSource(it2)) if it1 == it2 && on.fanOut(src1) == 1 && on.fanOut(src2) == 1 =>
          // we need to materialize the left
          val left = it1.toStream
          IteratorSource((left #::: left).iterator)
        case Merged(Nil) => IteratorSource(Iterator.empty)
        case Merged(single :: Nil) => single
        case Merged((src1 @ IteratorSource(it1)) :: (src2 @ IteratorSource(it2)) :: tail) if it1 != it2 && on.fanOut(src1) == 1 && on.fanOut(src2) == 1 =>
          Merged(IteratorSource(it1 ++ it2) :: tail)
        case Merged((src1 @ IteratorSource(it1)) :: (src2 @ IteratorSource(it2)) :: tail) if it1 == it2 && on.fanOut(src1) == 1 && on.fanOut(src2) == 1 =>
          // we need to materialize the left
          val left = it1.toStream
          Merged(IteratorSource((left #::: left).iterator) :: tail)
      }
    }

    object removeTag extends PartialRule[Flow] {
      def applyWhere[T](on: Dag[Flow]) = {
        case Tagged(in, _) => in
      }
    }

    /**
     * these are all optimization rules to simplify
     */
    val allRulesList: List[Rule[Flow]] =
      List(composeOptionMapped,
        composeConcatMap,
        mergePullDown,
        CombineMerges,
        removeTag,
        evalSource)

    val allRules = Rule.orElse(allRulesList)

    val ruleGen: Gen[Rule[Flow]] = {
      val allRules = List(composeOptionMapped, composeConcatMap, optionMapToConcatMap, mergePullDown, CombineMerges, evalSource, removeTag)
      for {
        n <- Gen.choose(0, allRules.size)
        gen = if (n == 0) Gen.const(List(Rule.empty[Flow])) else Gen.pick(n, allRules)
        rs <- gen
      } yield rs.reduce { (r1: Rule[Flow], r2: Rule[Flow]) => r1.orElse(r2) }
    }

    implicit val arbRule: Arbitrary[Rule[Flow]] =
      Arbitrary(ruleGen)

    def genFlow[T](g: Gen[T])(implicit cogen: Cogen[T]): Gen[Flow[T]] = {
      implicit val arb: Arbitrary[T] = Arbitrary(g)

      def genSource: Gen[Flow[T]] =
        Gen.listOf(g).map { l => Flow(l.iterator) }

      /**
       * We want to create DAGs, so we need to sometimes select a parent
       */
      def reachable(f: Flow[T]): Gen[Flow[T]] =
        Gen.lzy(Gen.oneOf(Flow.transitiveDeps(f).asInstanceOf[List[Flow[T]]]))

     val optionMap: Gen[Flow[T]] =
        for {
          parent <- Gen.lzy(genFlow(g))
          fn <- implicitly[Arbitrary[T => Option[T]]].arbitrary
        } yield parent.optionMap(fn)

      val concatMap: Gen[Flow[T]] =
        for {
          parent <- Gen.lzy(genFlow(g))
          fn <- implicitly[Arbitrary[T => List[T]]].arbitrary
        } yield parent.concatMap(fn)

      val merge: Gen[Flow[T]] =
        for {
          left <- Gen.lzy(genFlow(g))
          right <- Gen.frequency((3, genFlow(g)), (2, reachable(left)))
          swap <- Gen.choose(0, 1)
          res = if (swap == 1) (right ++ left) else (left ++ right)
        } yield res

      val tagged: Gen[Flow[T]] =
        for {
          tag <- g
          input <- genFlow(g)
        } yield input.tagged(tag)

      Gen.frequency((3, genSource), (1, optionMap), (1, concatMap), (1, tagged), (1, merge))
    }

    implicit def arbFlow[T: Arbitrary: Cogen]: Arbitrary[Flow[T]] =
      Arbitrary(genFlow[T](implicitly[Arbitrary[T]].arbitrary))


    def expDagGen[T: Cogen](g: Gen[T]): Gen[Dag[Flow]] = {
      val empty = Dag.empty[Flow](toLiteral)

      Gen.frequency((1, Gen.const(empty)), (10, genFlow(g).map { f => empty.addRoot(f)._1 }))
    }

    def arbExpDag[T: Arbitrary: Cogen]: Arbitrary[Dag[Flow]] =
      Arbitrary(expDagGen[T](implicitly[Arbitrary[T]].arbitrary))
  }
}

class DataFlowTest extends FunSuite {

  implicit val generatorDrivenConfig =
    PropertyCheckConfiguration(minSuccessful = 1000)
    //PropertyCheckConfiguration(minSuccessful = 100)

  import DataFlowTest._

  test("basic test 1") {
    val f1 = Flow((0 to 100).iterator)

    val branch1 = f1.map(_ * 2).filter(_ % 6 != 0)
    val branch2 = f1.map(_ * Int.MaxValue).filter(_ % 6 == 0)

    val tail = (branch1 ++ branch2).map(_ / 3)

    import Flow._

    val res = Dag.applyRule(tail, toLiteral, mergePullDown.orElse(composeOptionMapped))

    res match {
      case Merge(OptionMapped(s1, fn1), OptionMapped(s2, fn2)) =>
        assert(s1 == s2)
      case other => fail(s"$other")
    }
  }

  test("basic test 2") {
    def it1: Iterator[Int] = (0 to 100).iterator
    def it2: Iterator[Int] = (1000 to 2000).iterator

    val f = Flow(it1).map(_ * 2) ++ Flow(it2).filter(_ % 7 == 0)

    Dag.applyRule(f, Flow.toLiteral, Flow.allRules) match {
      case Flow.IteratorSource(it) =>
        assert(it.toList == (it1.map(_ * 2) ++ (it2.filter(_ % 7 == 0))).toList)
      case nonSrc =>
        fail(s"expected total evaluation $nonSrc")
    }

  }

  test("fanOut matches") {

    def law(f: Flow[Int], rule: Rule[Flow], maxApplies: Int) = {
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, maxApplies)
      val optF = optimizedDag.evaluate(id)

      val depGraph = SimpleDag[Flow[Any]](Flow.transitiveDeps(optF))(Flow.dependenciesOf _)

      def fanOut(f: Flow[Any]): Int = {
        val internal = depGraph.fanOut(f).getOrElse(0)
        val external = if (depGraph.isTail(f)) 1 else 0
        internal + external
      }

      optimizedDag.allNodes.foreach { n =>
        assert(optimizedDag.fanOut(n) == fanOut(n))
        assert(optimizedDag.isRoot(n) == (n == optF), s"$n should not be a root, only $optF is, $optimizedDag")
        assert(depGraph.isTail(n) == optimizedDag.isRoot(n), s"$n is seen as a root, but shouldn't, $optimizedDag")
      }
    }

    forAll(law(_, _, _))

    /**
     * Here we have a list of past regressions
     */
    val it1 = List(1, 2, 3).iterator
    val fn1 = { i: Int => if (i % 2 == 0) Some(i + 1) else None }
    val it2 = List(2, 3, 4).iterator
    val it3 = List(3, 4, 5).iterator
    val fn2 = { i: Int => None }
    val fn3 = { i: Int => (0 to i) }

    import Flow._

    val g = ConcatMapped(Merge(OptionMapped(IteratorSource(it1), fn1),
                               OptionMapped(Merge(IteratorSource(it2),IteratorSource(it3)), fn2)), fn3)
    law(g, Flow.allRules, 2)
  }

  test("we either totally evaluate or have Iterators with fanOut") {

    def law(f: Flow[Int], ap: Dag[Flow] => Dag[Flow]) = {
      val (dag, id) = Dag(f, Flow.toLiteral)
      val optDag = ap(dag)
      val optF = optDag.evaluate(id)

      optF match {
        case Flow.IteratorSource(_) => succeed
        case nonEval =>
          val depGraph = SimpleDag[Flow[Any]](Flow.transitiveDeps(nonEval))(Flow.dependenciesOf _)

          val fansOut = depGraph
            .nodes
            .collect {
              case src@Flow.IteratorSource(_) => src
            }
            .exists(depGraph.fanOut(_).get > 1)

          assert(fansOut, s"should have fanout: $nonEval")
      }
    }

    forAll(law(_: Flow[Int], { dag => dag(Flow.allRules) }))
    forAll(law(_: Flow[Int], { dag => dag.applySeq(Flow.allRulesList) }))
  }

  test("addRoot adds roots") {
    implicit val dag = Flow.arbExpDag[Int]

    forAll { (d: Dag[Flow], f: Flow[Int]) =>

      val (next, id) = d.addRoot(f)
      assert(next.isRoot(f))
      assert(next.evaluate(id) == f)
      assert(next.evaluate(next.idOf(f)) == f)
    }
  }

  test("all Dag.allNodes agrees with Flow.transitiveDeps") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      val optF = optimizedDag.evaluate(id)
      assert(optimizedDag.allNodes == Flow.transitiveDeps(optF).toSet, s"optimized: $optF $optimizedDag")
    }
  }

  test("transitiveDependenciesOf matches Flow.transitiveDeps") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      val optF = optimizedDag.evaluate(id)
      assert(optimizedDag.transitiveDependenciesOf(optF) == (Flow.transitiveDeps(optF).toSet - optF), s"optimized: $optF $optimizedDag")
    }
  }

  test("Dag: findAll(n).forall(evaluate(_) == n)") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      optimizedDag.allNodes.foreach { n =>
        optimizedDag.findAll(n).foreach { id =>
          assert(optimizedDag.evaluate(id) == n, s"$id does not eval to $n in $optimizedDag")
        }
      }
    }
  }

  test("apply the empty rule returns eq dag") {
    implicit val dag = Flow.arbExpDag[Int]

    forAll { (d: Dag[Flow]) =>
      assert(d(Rule.empty[Flow]) eq d)
    }
  }


  test("rules are idempotent") {
    def law(f: Flow[Int], rule: Rule[Flow]) = {
      val (dag, id) = Dag(f, Flow.toLiteral)
      val optimizedDag = dag(rule)
      val optF = optimizedDag.evaluate(id)

      val (dag2, id2) = Dag(optF, Flow.toLiteral)
      val optimizedDag2 = dag2(rule)
      val optF2 = optimizedDag2.evaluate(id2)

      assert(optF2 == optF, s"dag1: $optimizedDag -- dag1: $optimizedDag2")
    }

    forAll(law _)
  }

  test("dependentsOf matches SimpleDag.dependantsOf") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)
      val depGraph = SimpleDag[Flow[Any]](Flow.transitiveDeps(optimizedDag.evaluate(id)))(Flow.dependenciesOf _)

      optimizedDag.allNodes.foreach { n =>
        assert(optimizedDag.dependentsOf(n) == depGraph.dependantsOf(n).fold(Set.empty[Flow[Any]])(_.toSet))
        assert(optimizedDag.transitiveDependentsOf(n) ==
          depGraph.transitiveDependantsOf(n).toSet)
      }
    }
  }

  test("dependenciesOf matches toLiteral") {
    forAll { (f: Flow[Int]) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      def contract(n: Flow[_]): List[Flow[_]] =
        Flow.toLiteral(n) match {
          case Literal.Const(_) => Nil
          case Literal.Unary(n, _) => n.evaluate :: Nil
          case Literal.Binary(n1, n2, _) => n1.evaluate :: n2.evaluate :: Nil
          case Literal.Variadic(ns, _) => ns.map(_.evaluate)
        }

      dag.allNodes.foreach { n =>
        assert(dag.dependenciesOf(n) == contract(n))
      }
    }
  }

  test("hasSingleDependent matches fanOut") {
    forAll { (f: Flow[Int]) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      dag.allNodes.foreach { n =>
        assert(dag.hasSingleDependent(n) == (dag.fanOut(n) <= 1))
      }

      dag
        .allNodes
        .filter(dag.hasSingleDependent)
        .foreach { n =>
          assert(dag.dependentsOf(n).size <= 1)
        }
    }
  }

  test("contains(n) is the same as allNodes.contains(n)") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int, check: List[Flow[Int]]) =>
      val (dag, _) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      (optimizedDag.allNodes.iterator ++ check.iterator).foreach { n =>
        assert(optimizedDag.contains(n) == optimizedDag.allNodes(n), s"$n $optimizedDag")
      }
    }
  }

  test("all roots can be evaluated") {
    forAll { (roots: List[Flow[Int]], rule: Rule[Flow], max: Int) =>
      val dag = Dag.empty[Flow](Flow.toLiteral)

      // This is pretty slow with tons of roots, take 10
      val (finalDag, allRoots) = roots.take(10).foldLeft((dag, Set.empty[Id[Int]])) { case ((d, s), f) =>
        val (nextDag, id) = d.addRoot(f)
        (nextDag, s + id)
      }

      val optimizedDag = finalDag.applyMax(rule, max)

      allRoots.foreach { id =>
        assert(optimizedDag.evaluateOption(id).isDefined, s"$optimizedDag $id")
      }
    }
  }

  test("removeTag removes all .tagged") {
    forAll { f: Flow[Int] =>
      val (dag, id) = Dag(f, Flow.toLiteral)
      val optDag = dag(Flow.allRules) // includes removeTagged

      optDag.allNodes.foreach {
        case Flow.Tagged(_, _) => fail(s"expected no Tagged, but found one")
        case _ => succeed
      }
    }
  }

  test("reachableIds are only the set of nodes") {
    forAll { (f: Flow[Int], rule: Rule[Flow], max: Int) =>
      val (dag, id) = Dag(f, Flow.toLiteral)

      val optimizedDag = dag.applyMax(rule, max)

      assert(optimizedDag.reachableIds.map(optimizedDag.evaluate(_)) == optimizedDag.allNodes, s"$optimizedDag")
    }
  }

  test("adding explicit forks does not loop") {
    forAll { (f: Flow[Int]) =>
      Dag.applyRule(f, Flow.toLiteral, Flow.explicitFork)
      // we are just testing that this does not throw
    }

    // Here are some explicit examples:
    import Flow._
    val src = IteratorSource(Iterator(1))
    val example = ConcatMapped(Tagged(Merge(OptionMapped(src, { x: Int => Option(2 * x) }),src),0), { x: Int => List(x) })
    Dag.applyRule(example, Flow.toLiteral, Flow.explicitFork)

    // Here is an example where we have a root that has fanOut
    val d0 = Dag.empty(Flow.toLiteral)
    val (d1, id0) = d0.addRoot(src)
    val (d2, id1) = d1.addRoot(example)

    d2.apply(Flow.explicitFork)
  }
}
