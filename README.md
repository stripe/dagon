[![Build Status](https://api.travis-ci.org/stripe/dagon.svg)](https://travis-ci.org/stripe/dagon)
[![codecov.io](http://codecov.io/github/stripe/dagon/coverage.svg?branch=master)](http://codecov.io/github/stripe/dagon?branch=master)
[![Latest version](https://index.scala-lang.org/stripe/dagon/dagon-core/latest.svg?color=orange)](https://index.scala-lang.org/stripe/dagon/dagon-core)

## Dagon

> Dagon [...] is an ancient Mesopotamian Assyro-Babylonian and Levantine
> (Canaanite) deity. He appears to have been worshipped as a fertility
> god in Ebla, Assyria, Ugarit and among the Amorites. The Hebrew Bible
> mentions him as the national god of the Philistines with temples at
> Ashdod and elsewhere in Gaza.
>
> -- [Dagon Wikipedia entry](https://en.wikipedia.org/wiki/Dagon)

### Overview

Dagon is a library for rewriting
[directed acyclic graphs](https://en.wikipedia.org/wiki/Directed_acyclic_graph)
(i.e. DAGs).

### Quick Start

Dagon supports Scala 2.11, 2.12, and 2.13. It supports both the JVM
and JS platforms.

To use Dagon in your own project, you can include this snippet in
your `build.sbt` file:

```scala
// use this snippet for the JVM
libraryDependencies ++= List(
  "com.stripe" %% "dagon-core" % "0.3.3",
  compilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4"))

// use this snippet for JS, or cross-building
libraryDependencies ++= List(
  "com.stripe" %%% "dagon-core" % "0.3.3",
  compilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4"))
```

We strongly encourage you to use *kind-projector* with Dagon. Otherwise,
working with types like `FunctionK` will be signficantly more painful.

### Example

To use Dagon you will need the following things:

 * a DAG or AST type (e.g. `Eqn[T]` below).
 * a transformation from your DAG to Dagon's literal types (e.g. `toLiteral`)
 * some rewrite rules (e.g. `SimplifyNegation` and `SimplifyAddition`)

Dagon allows you to write very terse, natural rules that use partial
functions (similar to patttern-matching) to identify and transform
some AST "shapes" while leaving others alone. These patterns will all
be recursively applied until none of them match any part of the AST.

One consequence of this is that your rules should shrink the AST, or
at least simplify it in some sense. If your rules do not converge on a
final AST it's possible that the rewriter will not terminate (and will
loop forever on an ever-changing AST).

Here's a complete, working example of using Dagon:

```scala
object Example {

  import com.stripe.dagon._
  
  // 1. set up an AST type

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
    // these function constructors make the definition of
    // toLiteral a lot nicer.
    def negate[T]: Eqn[T] => Eqn[T] = Negate(_)
    def add[T]: (Eqn[T], Eqn[T]) => Eqn[T] = Add(_, _)
  }
  
  // 2. set up a transfromation from AST to Literal

  val toLiteral: FunctionK[Eqn, Literal[Eqn, ?]] =
    Memoize.functionK[Eqn, Literal[Eqn, ?]](
      new Memoize.RecursiveK[Eqn, Literal[Eqn, ?]] {
        def toFunction[T] = {
          case (c @ Const(_), f) => Literal.Const(c)
          case (v @ Var(_), f) => Literal.Const(v)
          case (Negate(x), f) => Literal.Unary(f(x), Eqn.negate)
          case (Add(x, y), f) => Literal.Binary(f(x), f(y), Eqn.add)
        }
      })
      
  // 3. set up rewrite rules

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

  // 4. apply rewrite rules to a particular AST value

  val a:  Eqn[Unit] = Var("x") + Const(1)
  val b1: Eqn[Unit] = a + Const(2)
  val b2: Eqn[Unit] = a + Const(5) + Var("y")
  val c:  Eqn[Unit] = b1 - b2

  val simplified: Eqn[Unit] =
    Dag.applyRule(c, toLiteral, rules)
}
```

Dagon assumes your AST is paramterized on a `T` type. If yours is not,
you can create a new type of the correct shape using a phantom type:

```scala
sealed trait Ast
...

object Ast {
  // T is a "phantom type" -- it's not actually used in the type alias.
  type Phantom[T] = Ast
}

val toLiteral: FunctionK[Ast.Phantom, Literal[Ast.Phantom, ?]] = ...
```

### Implementing toLiteral

The function `toLiteral` has the type `FunctionK[N, Literal[N, ?]]`.
This means that it can produce a `N[T] => Literal[N, T]`. The type
`N[_]` is your AST type; in the example it was `Eqn[_]`.

Dagon's `Literal` is sealed and has three subtypes:

 * `Literal.Const(leaf)`: a `leaf` node of your AST
 * `Literal.Unary(node, f)`: a child `node` and a unary function `f`
 * `Literal.Binary(lhs, rhs, g)`: two nodes (`lhs`, `rhs`) and a binary function `g`

The functions `f` and `g` are mapping from inputs of type `N[T1]` to
outputs of type `N[T2]` (where `N[_]` is your AST type). In the
example above `T1` and `T2` are both `Unit`.

It's important that your `toLiteral` function is invertible. That
means that the following should be true:

```scala
val node: Ast[T] = ...
toLiteral[T](node).evaluate == node
```

### Future Work

Here are some directions possible future work could take:

 * Producing laws to generate and test your AST values against these
   rewrites. Many of the tests we use internally could be generalized
   and exported for third-party use.

 * Cost-based optimization: right now rules are applied until they
   don't match, which means that rules need to be conservative, and
   should not expand the size of the graph. Some rules could locally
   increase graph size but result in smaller graphs overall. One
   example of this would be *arithmetic distribution*, e.g. rewriting
   `x * (y + z)` into `x * z + y * z`.
   
 * Benchmarking and performance optimization. While this code performs
   adequately for most real-world use cases it's likely quadratic or
   super-quadratic in the worst-case. We could likely optimize some of
   the algorithms we are using as well as the actual code involved.

### Copyright and License

Dagon is available to you under the [Apache License, version 2](https://www.apache.org/licenses/LICENSE-2.0).

Copyright 2017 Stripe.

Derived from [Summingbird](https://github.com/twitter/summingbird), which is copyright 2013-2017 Twitter.
