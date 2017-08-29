package readme

object Example {

  import com.stripe.dagon._

  sealed trait Eqn[T] {
    def unary_-(): Eqn[T] = Negate(this)
    def +(that: Eqn[T]): Eqn[T] = Add(this, that)
    def -(that: Eqn[T]): Eqn[T] = Add(this, Negate(that))
  }

  case class Const[T](value: Int) extends Eqn[T]
  case class Var[T](name: String) extends Eqn[T]
  case class Negate[T](eqn: Eqn[T]) extends Eqn[T]
  case class Add[T](lhs: Eqn[T], rhs: Eqn[T]) extends Eqn[T]

  object Eqn {
    def negate[T]: Eqn[T] => Eqn[T] = Negate(_)
    def add[T]: (Eqn[T], Eqn[T]) => Eqn[T] = Add(_, _)
  }

  val toLiteral: FunctionK[Eqn, Literal[Eqn, ?]] =
    Memoize.functionK[Eqn, Literal[Eqn, ?]](new Memoize.RecursiveK[Eqn, Literal[Eqn, ?]] {
      def toFunction[T] = {
        case (c @ Const(_), f) => Literal.Const(c)
        case (v @ Var(_), f) => Literal.Const(v)
        case (Negate(x), f) => Literal.Unary(f(x), Eqn.negate)
        case (Add(x, y), f) => Literal.Binary(f(x), f(y), Eqn.add)
      }
    })

  object SimplifyNegation extends PartialRule[Eqn] {
    def applyWhere[T](on: Dag[Eqn]) = {
      case Negate(Negate(e)) => e
      case Negate(Const(x)) => Const(-x)
    }
  }

  object SimplifyAddition extends PartialRule[Eqn] {
    def applyWhere[T](on: Dag[Eqn]) = {
      case Add(Const(x), Const(y)) => Const(x + y)
      case Add(Add(e, Const(x)), Const(y)) => Add(e, Const(x + y))
      case Add(Add(Const(x), e), Const(y)) => Add(e, Const(x + y))
      case Add(Const(x), Add(Const(y), e)) => Add(Const(x + y), e)
      case Add(Const(x), Add(e, Const(y))) => Add(Const(x + y), e)
    }
  }

  val rules = SimplifyNegation.orElse(SimplifyAddition)

  val a: Eqn[Unit] = Var("x") + Const(1)
  val b1 = a + Const(2)
  val b2 = a + Const(5) + Var("y")
  val c = b1 - b2

  Dag.applyRule(c, toLiteral, rules)
}
