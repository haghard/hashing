package pipeline

object Pipeline2 {

  import cats.Functor
  import cats.instances.option._

  object implicits {
    import shapeless._

    //pitfall!!! if this is a val Ops won't work
    implicit def PNil: Flow.Aux[CNil, CNil, CNil, Unit :+: CNil] = {
      new Flow[CNil, CNil, CNil] {
        type Out = Unit :+: CNil
        override def apply(input: String): Out = Inl(())
      }
    }

    implicit def inductivePipeline[TH, F[_], AH, BH, TT <: Coproduct, AT <: Coproduct, BT <: Coproduct, OT <: Coproduct](implicit head: Flow.Aux[TH, AH, BH, Flow.Out[F]], tail: Flow.Aux[TT, AT, BT, OT]): Flow.Aux[TH :+: TT, AH :+: AT, BH :+: BT, F[Unit] :+: OT] = {
      new Flow[TH :+: TT, AH :+: AT, BH :+: BT] {
        final override type Out = F[Unit] :+: OT
        final override def apply(input: String): Out = {
          head(input).fold({ _ ⇒ Inr(tail(input)) }, s ⇒ Inl(s))
        }
      }
    }

    implicit class Ops[TT <: Coproduct, AT <: Coproduct, BT <: Coproduct, OT <: Coproduct](val tail: Flow.Aux[TT, AT, BT, OT]) extends AnyVal {
      def +:[TH, F[_], AH, BH](head: Flow.Aux[TH, AH, BH, Flow.Out[F]]): Flow.Aux[TH :+: TT, AH :+: AT, BH :+: BT, F[Unit] :+: OT] =
        inductivePipeline[TH, F, AH, BH, TT, AT, BT, OT](head, tail)
    }
  }

  //- Given an ordered extensional set of Pipelines
  //- Produced a computation that is *at most 1* Pipeline
  //- Given a Product, Produce a Coproduct and an Output type

  trait Algorithm[-T] {
    def name: String
  }

  trait Source[F[_], In] extends (String ⇒ F[In])

  object Source {
    def apply[F[_], In](f: String ⇒ F[In]) = new Source[F, In] {
      override def apply(v: String): F[In] = f(v)
    }
  }

  trait Transformation[In, Out] extends (In ⇒ Out)

  object Transformation {
    def apply[In, Out](f: In ⇒ Out) = new Transformation[In, Out] {
      override def apply(v: In): Out = f(v)
    }
  }

  trait Sink[Out] extends (Out ⇒ Unit)

  object Sink {
    def apply[Out](f: Out ⇒ Unit) = new Sink[Out] {
      override def apply(v: Out): Unit = f(v)
    }
  }

  sealed trait Flow[-T, A, B] {
    type Out
    def apply(uri: String): Out
  }

  object Flow {
    type Out[F[_]] = Either[Unit, F[Unit]]
    type Default[T, F[_], A, B] = Aux[T, A, B, Out[F]]
    type Aux[T, A, B, O] = Flow[T, A, B] { type Out = O }

    final def apply[T: Algorithm, F[_]: Functor, A, B](implicit
      read: Source[F, A],
                                                       T: Transformation[A, B], write: Sink[B]): Default[T, F, A, B] = {
      val G: Algorithm[T] = implicitly
      val F: Functor[F] = implicitly
      new Flow[T, A, B] {
        final override type Out = Flow.Out[F]
        override def apply(input: String): Out = {
          val b = input.contains(G.name)
          println(s"$input matches(${G.name}) = $b")
          if (b) Right {
            val in = read(input)
            val computed = F.map(in)(T)
            F.map(computed)(write)
          }
          else Left(())
        }
      }
    }
  }

  trait One
  trait Two
  trait Three

  object allImplicits {
    implicit val a = new Algorithm[One] { override val name = "one" }
    implicit val b = new Algorithm[Two] { override val name = "two" }
    implicit val c = new Algorithm[Three] { override val name = "lenght" }

    implicit val src = Source[Option, Int] { (line: String) ⇒ Option(line.length) }
    implicit val map = Transformation { (v: Int) ⇒ v * -1 }
    implicit val consoleSink = Sink[Int] { (v: Int) ⇒ println(s"out > $v") }
  }

  import implicits._

  def blockA: Flow.Aux[One, Int, Int, Either[Unit, Option[Unit]]] = {
    import allImplicits._
    Flow[One, Option, Int, Int]
  }

  def blockB: Flow.Aux[Two, Int, Int, Either[Unit, Option[Unit]]] = {
    import allImplicits._
    Flow[Two, Option, Int, Int]
  }

  def blockC: Flow.Aux[Three, Int, Int, Either[Unit, Option[Unit]]] = {
    import allImplicits._
    Flow[Three, Option, Int, Int]
  }

  val chain = blockA +: blockB +: blockC +: PNil

  val failure = chain("aaa")
  println("*******************")
  val success0 = chain("two")
  println("*******************")
  val success1 = chain("lenght")
  println("*******************")
}
