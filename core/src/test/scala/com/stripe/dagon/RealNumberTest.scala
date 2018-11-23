package com.stripe.dagon

import org.scalatest.FunSuite
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks._

object RealNumbers {

  class SortedList[+A] private (val toList: List[A]) {
    def apply[A1 >: A](a: A1): Boolean =
      toList.contains(a)

    def filter(fn: A => Boolean): SortedList[A] =
      new SortedList(toList.filter(fn))

    def collect[B: Ordering](fn: PartialFunction[A, B]): SortedList[B] =
      new SortedList(toList.collect(fn).sorted)

    def |[A1 >: A: Ordering](that: SortedList[A1]): SortedList[A1] =
      // could do a merge-sort here in linear time
      new SortedList((toList reverse_::: that.toList).sorted)

    def +[A1 >: A: Ordering](a: A1): SortedList[A1] =
      new SortedList((a :: toList).sorted)

    def -[A1 >: A](a: A1): SortedList[A1] =
      filter(_ != a)

    def ++[A1 >: A: Ordering](that: Iterable[A1]): SortedList[A1] =
      new SortedList((toList ++ that).sorted)

    override def equals(that: Any) =
      that match {
        case sl: SortedList[_] => toList == sl.toList
        case _ => false
      }
    override def hashCode: Int = toList.hashCode
  }
  object SortedList {
    val empty: SortedList[Nothing] = new SortedList(Nil)

    def apply[A: Ordering](as: A*): SortedList[A] =
      new SortedList(as.toList.sorted)

    implicit def sortedListOrd[A: Ordering]: Ordering[SortedList[A]] = {
      val ordList: Ordering[Iterable[A]] = Ordering.Iterable[A]
      ordList.on { ls: SortedList[A] => ls.toList }
    }

    // Get all the list methods
    implicit def toList[A](sl: SortedList[A]): List[A] = sl.toList
  }

  sealed abstract class Real { self: Product =>
    import Real._

    // cache the hashcode
    override val hashCode = scala.util.hashing.MurmurHash3.productHash(self)
    override def equals(that: Any) = that match {
      case thatF: Real =>
        if (thatF eq this) true
        else if (thatF.hashCode != hashCode) false
        else {
          @annotation.tailrec
          def loop(todo: List[RefPair[Real, Real]], seen: Set[RefPair[Real, Real]]): Boolean =
            todo match {
              case Nil => true
              case rf :: tail =>
                if (rf.itemsEq) loop(tail, seen)
                else rf match {
                  case RefPair(Const(a), Const(b)) =>
                    (a == b) && loop(tail, seen)
                  case RefPair(Variable(a), Variable(b)) =>
                    (a == b) && loop(tail, seen)
                  case RefPair(Sum(as), Sum(bs)) =>
                    (as.size == bs.size) && {
                      val stack =
                        as.iterator.zip(bs.iterator).map { case (a, b) =>
                          RefPair(a, b)
                        }
                        .filterNot(seen)
                        .toList

                      loop(stack reverse_::: tail, seen ++ stack)
                    }
                  case RefPair(Prod(as), Prod(bs)) =>
                    (as.size == bs.size) && {
                      val stack =
                        as.iterator.zip(bs.iterator).map { case (a, b) =>
                          RefPair(a, b)
                        }
                        .filterNot(seen)
                        .toList

                      loop(stack reverse_::: tail, seen ++ stack)
                    }
                  case _ => false
                }
            }
          loop(RefPair[Real, Real](this, thatF) :: Nil, Set.empty)
        }
      case _ => false
    }

    def expand: Real =
      this match {
        case Const(_) | Variable(_) => this
        case Sum(s) => Real.sum(SortedList(s.map(_.expand): _*))
        case Prod(p) =>
          // for all the sums in here we need do the full cross product
          val nonSums = p.filter(_.toSum.isEmpty)
          val sums = p.toList.collect { case Sum(s) => s.toList }
          def cross(ls: List[List[Real]]): List[Real] =
            //(a + b), (c + d)... = a * cross(tail) + b * cross(tail) ...
            ls match {
              case Nil => Const(1.0) :: Nil
              case h :: tail =>
                cross(tail).flatMap { term =>
                  h.map { h => Real.prod(SortedList((h :: term :: Nil): _*)) }
                }
            }
         val sum1 = Real.sum(SortedList(cross(sums): _*))
         Real.prod(Prod(nonSums), sum1)
      }

    def evaluate(m: Map[String, Double]): Option[Double] =
      this match {
        case Const(d) => Some(d)
        case Variable(v) => m.get(v)
        case Sum(s) =>
          s.iterator.foldLeft(Option(0.0)) {
            case (None, _) => None
            case (Some(d), v) => v.evaluate(m).map(_ + d)
          }
        case Prod(p) =>
          p.iterator.foldLeft(Option(1.0)) {
            case (None, _) => None
            case (Some(d), v) => v.evaluate(m).map(_ * d)
          }
      }

    def freeVars: Set[String] =
      this match {
        case Const(_) => Set.empty
        case Variable(v) => Set(v)
        case Sum(v) => v.iterator.flatMap(_.freeVars).toSet
        case Prod(v) => v.iterator.flatMap(_.freeVars).toSet
      }

    def toSum: Option[Sum] =
      this match {
        case s@Sum(_) => Some(s)
        case _ => None
      }
    def toProd: Option[Prod] =
      this match {
        case p@Prod(_) => Some(p)
        case _ => None
      }
    def toConst: Option[Const] =
      this match {
        case c@Const(_) => Some(c)
        case _ => None
      }

    def divOpt(r: Real): Option[Real] =
      this match {
        case Prod(inner) if inner(r) => Some(prod(inner - r))
        case self if r == self => Some(one)
        case _ => None
      }

    override def toString = {
      def loop(r: Real): String =
        r match {
          case Variable(x) => x
          case Const(d) => d.toString
          case Sum(s) =>
            s.iterator.map(loop(_)).mkString("(", " + ", ")")
          case Prod(p) =>
            p.iterator.map(loop(_)).mkString("(", "*", ")")
        }
      loop(this)
    }

    def cost: Int =
      this match {
        case Variable(_) | Const(_) => 1
        case Sum(s) =>
          s.iterator.map(_.cost).sum
        case Prod(p) =>
          p.iterator.map(_.cost).sum
      }
  }
  object Real {
    case class Const(toDouble: Double) extends Real
    case class Variable(name: String) extends Real
    // use a sorted set, we have unique representations
    case class Sum(terms: SortedList[Real]) extends Real
    case class Prod(terms: SortedList[Real]) extends Real

    val zero: Real = Const(0.0)
    val one: Real = Const(1.0)

    def const(d: Double): Real = Const(d)
    def variable(v: String): Real = Variable(v)

    def sum(s: SortedList[Real]): Real =
      if (s.isEmpty) zero
      else if (s.size == 1) s.head
      else Sum(s)

    def prod(a: Real, b: Real): Real =
      Prod(SortedList(a, b))

    def prod(s: SortedList[Real]): Real =
      if (s.isEmpty) one
      else if (s.size == 1) s.head
      else Prod(s)

    implicit def ordReal[R <: Real]: Ordering[R] =
      new Ordering[R] {
        def compareIt(a: Iterator[Real], b: Iterator[Real]): Int = {
          @annotation.tailrec
          def loop(): Int =
            (a.hasNext, b.hasNext) match {
              case (true, true) =>
                val c = compareReal(a.next, b.next)
                if (c == 0) loop() else c
              case (false, true) => -1
              case (true, false) => 1
              case (false, false) => 0
            }

          loop()
        }
        def compare(a: R, b: R) = compareReal(a, b)

        def compareReal(a: Real, b: Real) =
          (a, b) match {
            case (Const(a), Const(b)) => java.lang.Double.compare(a, b)
            case (Const(_), _) => -1
            case (Variable(a), Variable(b)) => a.compareTo(b)
            case (Variable(_), Const(_)) => 1
            case (Variable(_), _) => -1
            case (Sum(a), Sum(b)) => compareIt(a.iterator, b.iterator)
            case (Sum(_), Const(_) | Variable(_)) => 1
            case (Sum(_), Prod(_)) => -1
            case (Prod(a), Prod(b)) => compareIt(a.iterator, b.iterator)
            case (Prod(_), Const(_) | Variable(_) | Sum(_)) => 1
          }
      }

    def genReal(depth: Int): Gen[Real] = {
      val const = Gen.choose(-1000, 1000).map { i => Const(i.toDouble) }
      val variable = Gen.choose('a', 'z').map { v => Variable(v.toString) }
      if (depth <= 0) Gen.oneOf(const, variable)
      else {
        val rec = Gen.lzy(genReal(depth - 1))
        val items = Gen.choose(0, 10).flatMap(Gen.listOfN(_, rec))
        val sum = items.map { ls => Real.sum(SortedList(ls: _*)) }
        val prod = items.map { ls => Real.prod(SortedList(ls: _*)) }
        Gen.oneOf(const, variable, sum, prod)
      }
    }

    implicit val arbReal: Arbitrary[Real] = Arbitrary(genReal(4))
  }

  type RealN[A] = Real
  def toLiteral: FunctionK[RealN, Literal[RealN, ?]] =
    Memoize.functionK[RealN, Literal[RealN, ?]](new Memoize.RecursiveK[RealN, Literal[RealN, ?]] {
      import Real._
      def toFunction[T] = {
        case (r@(Const(_) | Variable(_)), _) => Literal.Const(r)
        case (Sum(rs), rec) =>
          Literal.Variadic[RealN, T, T](rs.iterator.map(rec[T](_)).toList, { rs => Sum(SortedList(rs: _*)) })
        case (Prod(rs), rec) =>
          Literal.Variadic[RealN, T, T](rs.iterator.map(rec[T](_)).toList, { rs => Prod(SortedList(rs: _*)) })
      }
    })


  sealed trait Parser[+A] {
    def apply(s: String): Option[(String, A)]
    def map[B](fn: A => B): Parser[B] = Parser.Map(this, fn)
    def zip[B](that: Parser[B]): Parser[(A, B)] = Parser.Zip(this, that)
    def |[A1 >: A](that: Parser[A1]): Parser[A1] =
      (this, that) match {
        case (Parser.OneOf(l), Parser.OneOf(r)) => Parser.OneOf(l ::: r)
        case (l, Parser.OneOf(r)) => Parser.OneOf(l :: r)
        case (Parser.OneOf(l), r) => Parser.OneOf(l :+ r)
        case (l, r) => Parser.OneOf(List(l, r))
      }

    def ? : Parser[Option[A]] =
      map(Some(_)) | Parser.Pure(None)

    def *>[B](that: Parser[B]): Parser[B] =
      zip(that).map(_._2)

    def <*[B](that: Parser[B]): Parser[A] =
      zip(that).map(_._1)
  }

  object Parser {
    final case class Pure[A](a: A) extends Parser[A] {
      def apply(s: String) = Some((s, a))
    }
    final case class Map[A, B](p: Parser[A], fn: A => B) extends Parser[B] {
      def apply(s: String) = p(s).map { case (s, a) => (s, fn(a)) }
    }
    final case class Zip[A, B](a: Parser[A], b: Parser[B]) extends Parser[(A, B)] {
      def apply(s: String) = a(s).flatMap { case (s, a) => b(s).map { case (s, b) => (s, (a, b)) } }
    }

    final case class OneOf[A](ls: List[Parser[A]]) extends Parser[A] {
      def apply(s: String) = {
        @annotation.tailrec
        def loop(ls: List[Parser[A]]): Option[(String, A)] =
          ls match {
            case Nil => None
            case h :: tail =>
              h(s) match {
                case None => loop(tail)
                case some => some
              }
          }
        loop(ls)
      }
    }

    final case class Rep[A](a: Parser[A]) extends Parser[List[A]] {
      def apply(str: String) = {
        @annotation.tailrec
        def loop(str: String, acc: List[A]): (String, List[A]) =
          a(str) match {
            case None => (str, acc.reverse)
            case Some((rest, a)) => loop(rest, a :: acc)
          }

        Some(loop(str, Nil))
      }
    }

    final case class StringParser(expect: String) extends Parser[String] {
      val len = expect.length
      def apply(s: String) =
        if (s.startsWith(expect)) Some((s.drop(len), expect))
        else None
    }

    final case class LazyParser[A](p: () => Parser[A]) extends Parser[A] {
      private lazy val pa: Parser[A] = {
        @annotation.tailrec
        def loop(p: Parser[A]): Parser[A] =
          p match {
            case LazyParser(lp) => loop(lp())
            case nonLazy => nonLazy
          }

        loop(p())
      }

      def apply(s: String) = pa(s)
    }

    def str(s: String): Parser[String] = StringParser(s)
    def chr(c: Char): Parser[String] = StringParser(c.toString)
    def number(n: Int): Parser[Int] = StringParser(n.toString).map(_ => n)
    def defer[A](p: => Parser[A]): Parser[A] = LazyParser(() => p)
  }

  val realParser: Parser[Real] = {
    val variable: Parser[Real] =
      Parser.OneOf(
        ('a' to 'z').toList.map(Parser.chr(_))
      ).map(Real.Variable(_))

    val digit: Parser[Int] = Parser.OneOf((0 to 9).toList.map(Parser.number(_)))
    val intP: Parser[Double] =
      digit
        .zip(Parser.Rep(digit))
        .map {
          case (d, ds) =>
            (d :: ds).foldLeft(0.0) { (acc, d) => acc * 10.0 + d }
        }

    val constP =
      (Parser.chr('-').?.zip(intP.zip((Parser.chr('.') *> Parser.Rep(digit)).?)))
        .map {
          case (s, (h, None)) => s.fold(h)(_ => -h)
          case (s, (h, Some(rest))) =>
            val num = rest.reverse.foldLeft(0.0) { (acc, d) => acc / 10.0 + d }
            val pos = h + (num / 10.0)
            s.fold(pos)(_ => -pos)
        }
        .map(Real.const(_))

    val recurse = Parser.defer(realParser)

    def op(str: String): Parser[SortedList[Real]] = {
      val left = Parser.chr('(')
      val right = Parser.chr(')')
      val rest = Parser.Rep(Parser.str(str) *> recurse)
      (left *> recurse.zip(rest) <* right)
        .map {
          case (h, t) => SortedList((h :: t) :_*)
        }
    }

    variable | constP | op(" + ").map(Real.sum(_)) | op("*").map(Real.prod(_))
  }

    object CombineProdSum extends Rule[RealN] {
      import Real._

      def apply[A](dag: Dag[RealN]) = {
        case Sum(inner) if inner.exists(_.toSum.isDefined) =>
          val nonSum = inner.filter(_.toSum.isEmpty)
          val innerSums = inner.flatMap(_.toSum match {
            case Some(Sum(s)) => s
            case None => SortedList.empty
          })
          Some(sum(nonSum ++ innerSums))

        case Prod(inner) if inner.exists(_.toProd.isDefined) =>
          val nonProd = inner.filter(_.toProd.isEmpty)
          val innerProds = inner.flatMap(_.toProd match {
            case Some(Prod(s)) => s
            case None => SortedList.empty
          })
          Some(prod(nonProd ++ innerProds))

        case _ => None
      }
    }

    object CombineConst extends Rule[RealN] {
      import Real._

      def apply[A](dag: Dag[RealN]) = {
        case Sum(inner) if inner.count(_.toConst.isDefined) > 1 =>
          val nonConst = inner.filter(_.toConst.isEmpty)
          val c = inner.collect { case Const(d) => d }.sum
          Some(sum(nonConst + Const(c)))
        case Prod(inner) if inner.count(_.toConst.isDefined) > 1 =>
          val nonConst = inner.filter(_.toConst.isEmpty)
          val c = inner.collect { case Const(d) => d }.product
          Some(prod(nonConst + Const(c)))
        case _ => None
      }
    }

    object RemoveNoOp extends Rule[RealN] {
      import Real._
      def apply[A](dag: Dag[RealN]) = {
        case Sum(rs) if rs.collectFirst { case Const(0.0) => () }.nonEmpty =>
          Some(sum(rs.filter(_ != Const(0.0))))
        case Prod(rs) if rs.collectFirst { case Const(1.0) => () }.nonEmpty =>
          Some(prod(rs.filter(_ != Const(1.0))))
        case _ => None
      }
    }

    /*
     * The idea here is to take (ab + ac) to a(b + c)
     *
     */
    object ReduceProd extends Rule[RealN] {
      import Real._

      def apply[A](dag: Dag[RealN]) = {
        case Sum(maybeProd) if maybeProd.count(_.toProd.isDefined) > 1 =>
          // take the union of all the products:
          val prods = maybeProd.collect { case p@Prod(_) => p }
          val nonProds = maybeProd.filter(_.toProd.isEmpty)
          val allProds = prods.iterator.map(_.terms.toList.toSet).reduce(_ | _) ++ nonProds
          // each of the products are candidates for reducing:
          val candidates = allProds.iterator.map { p =>
              val divO = prods.toList.map { pr => (pr.divOpt(p), pr) }
              val canDiv = divO.collect { case (Some(res), _) => res }
              val noDiv = divO.collect { case (None, pr) => pr }
              (p, canDiv, noDiv)
            } // here we are just going to take the biggest win
            .filter { case (_, canDiv, _) => canDiv.size > 1 }

          if (candidates.isEmpty) None
          else {
            val (p, canDiv, noDiv) =
              candidates.maxBy { case (_, canDiv, _) => canDiv.size }
            /*
             * p*canDiv + noDiv
             */
            Some(sum((nonProds ++ noDiv) +
              prod(SortedList(p, sum(SortedList(canDiv.toList: _*))))))
          }
        case _ => None
      }
    }

  val allRules: Rule[RealN] =
    CombineConst orElse CombineProdSum orElse RemoveNoOp orElse ReduceProd

  /**
   * Unsafe string parsing, to be used in testing
   */
  def real(s: String): Real =
    realParser(s) match {
      case None => sys.error(s"couldn't parse: $s")
      case Some(("", r)) => r
      case Some((rest, _)) => sys.error(s"still need to parse: $rest")
    }

  def optimizeAll(r: Real): Real = {
    val (dag, id) = Dag[Any, RealN](r, toLiteral)
    var seen: Set[Real] = Set(dag.evaluate(id))

    def loop(d: Dag[RealN]): Dag[RealN] = {
      val d1 = d.applyOnce(allRules)
      val r1 = d1.evaluate(id)
      if (d1 == d) d1
      else if (seen(r1)) {
        println(s"loop: $r from ${d.evaluate(id)} to ${r1}")
        d1
      } else {
        seen += r1
        loop(d1)
      }
    }
    val optDag = loop(dag)
    optDag.evaluate(id)
  }
}

class RealNumberTest extends FunSuite {
  import RealNumbers._

  implicit val generatorDrivenConfig =
   //PropertyCheckConfiguration(minSuccessful = 5000)
   PropertyCheckConfiguration(minSuccessful = 100)

   test("can parse") {
     assert(real("1") == Real.Const(1.0))
     assert(real("1.0") == Real.Const(1.0))
     assert(real("1.5") == Real.Const(1.5))
     assert(real("-1.5") == Real.Const(-1.5))
     assert(real("x") == Real.Variable("x"))
     assert(real("(1 + 2)") == Real.sum(SortedList(Real.const(1.0), Real.const(2.0))))
     assert(real("(1*2)") == Real.prod(SortedList(Real.const(1.0), Real.const(2.0))))
   }

   test("combine const") {
     assert(optimizeAll(real("(1 + 2 + 3)")) == real("6"))
   }

   test("we can parse anything") {
     forAll { r: Real =>
       assert(real(r.toString) == r, s"couldn't parse: $r")
     }
   }

   test("optimization reduces cost") {
     def law(r: Real, strong: Boolean) = {
       val optR = optimizeAll(r)
       if (strong) assert(r.cost > optR.cost, s"$r => $optR")
       else assert(r.cost >= optR.cost, s"$r => $optR")
     }
     forAll(Real.genReal(4))(law(_, false))

     val strongCases = List(
       "((x*1) + (x*2))",
       "((x*1) + (x*2) + (y*3))",
       "(1 + 2)")

     strongCases.foreach { s => law(real(s), true) }
   }

   test("optimization does not change evaluation") {
     val genMap = Gen.mapOf(Gen.zip(Gen.choose('a', 'z').map(_.toString), Gen.choose(-1000.0, 1000.0)))

     forAll(Real.genReal(3), genMap)  { (r, vars) =>
       val optR = optimizeAll(r)
       (optR.evaluate(vars), r.evaluate(vars)) match {
         case (None, None) => succeed
         case (Some(_), None) => fail(s"optimized succeded: $r, $optR")
         case (None, Some(_)) => fail(s"unoptimized succeded: $r, $optR")
         case (Some(a), Some(b)) =>
           val diff = Math.abs(a - b)
           if (diff < 1e-6) succeed
           else {
             assert(diff/(Math.abs(a) + Math.abs(b)) <= 1e-6, s"$optR for $r: $a, $b")
           }
       }
     }
   }

   test("evaluation works when all vars are present") {
     val genMap = Gen.mapOf(Gen.zip(Gen.choose('a', 'z').map(_.toString), Gen.choose(-1000.0, 1000.0)))
     forAll(Real.genReal(5), genMap) { (r, vars) =>
       r.evaluate(vars) match {
         case None => assert((r.freeVars -- vars.keys).nonEmpty)
         case Some(_) => assert((r.freeVars -- vars.keys).isEmpty)
       }
     }
   }

   test("optimization de-foils") {
     val sum = Gen.choose(1, 4).flatMap(Gen.listOfN(_, Real.genReal(0)))
       .map { ls => Real.sum(SortedList(ls: _*)) }
     val prod = Gen.choose(1, 4).flatMap(Gen.listOfN(_, sum))
       .map { ls => Real.prod(SortedList(ls: _*)) }
     // these products of sums
     forAll(prod) { r: Real =>
       val cost0 = r.cost
       val expanded = r.expand
       val optR = optimizeAll(r)
       assert(optR.cost <= cost0, s"$r optimized to $optR expanded to: $expanded")
     }
   }
}
