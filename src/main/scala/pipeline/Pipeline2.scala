package pipeline

/*
 The idea comes from https://github.com/dreadedsoftware/talks/tree/master/tls2017

 runMain pipeline.Pipeline2
 */
object Pipeline2 {

  import cats.Functor
  import cats.instances.option._
  import shapeless._

  type Payload[F[_], T] =
    shapeless.:+:[F[T], shapeless.:+:[F[T], shapeless.:+:[F[T], shapeless.:+:[Unit, shapeless.CNil]]]]

  object implicits {
    import shapeless._

    //pitfall!!! if this is a val Ops won't work
    implicit def PNil: Flow.Aux[CNil, CNil, CNil, Unit :+: CNil] =
      new Flow[CNil, CNil, CNil] {
        type Out = Unit :+: CNil
        override def apply(alg: String, payload: String): Out = Inl(())
      }

    implicit def inductivePipeline[TH, F[_], AH, BH, TT <: Coproduct, AT <: Coproduct, BT <: Coproduct, OT <: Coproduct](
      implicit head: Flow.Aux[TH, AH, BH, Flow.Out[F, BH]],
      tail: Flow.Aux[TT, AT, BT, OT]
    ): Flow.Aux[TH :+: TT, AH :+: AT, BH :+: BT, F[BH] :+: OT] =
      new Flow[TH :+: TT, AH :+: AT, BH :+: BT] {
        final override type Out = F[BH] :+: OT
        final override def apply(alg: String, payload: String): Out =
          head(alg, payload).fold({ _ ⇒
            Inr(tail(alg, payload))
          }, s ⇒ Inl(s))
      }

    implicit class Ops[TT <: Coproduct, AT <: Coproduct, BT <: Coproduct, OT <: Coproduct](
      val tail: Flow.Aux[TT, AT, BT, OT]
    ) extends AnyVal {
      def +:[TH, F[_], AH, BH](
        head: Flow.Aux[TH, AH, BH, Flow.Out[F, BH]]
      ): Flow.Aux[TH :+: TT, AH :+: AT, BH :+: BT, F[BH] :+: OT] =
        inductivePipeline[TH, F, AH, BH, TT, AT, BT, OT](head, tail)
    }
  }

  //- Given an ordered extensional set of pipelines
  //- Produced a computation that is *at most 1* Pipeline

  trait Algorithm[-T] {
    def name: String
  }

  trait Source[F[_], In] extends (String ⇒ F[In])

  object Source {
    def apply[F[_], In](f: String ⇒ F[In]): Source[F, In] = (v: String) ⇒ f(v)
  }

  trait Transformation[In, Out] extends (In ⇒ Out)

  object Transformation {
    def apply[In, Out](f: In ⇒ Out): Transformation[In, Out] = (v: In) ⇒ f(v)
  }

  trait Sink[Out] extends (Out ⇒ Unit)

  object Sink {
    def apply[Out](f: Out ⇒ Unit): Sink[Out] = (v: Out) ⇒ f(v)
  }

  sealed trait Flow[-T, A, B] {
    type Out
    def apply(alg: String, payload: String): Out
  }

  object Flow {
    //type Out[F[_]]              = Either[Unit, F[Unit]]
    //type Default[T, F[_], A, B] = Aux[T, A, B, Out[F]]
    //type Aux[T, A, B, O] = Flow[T, A, B] { type Out = O }

    type Out[F[_], B]           = Either[Unit, F[B]]
    type Default[T, F[_], A, B] = Aux[T, A, B, Either[Unit, F[B]]]
    type Aux[T, A, B, O]        = Flow[T, A, B] { type Out = O }

    final def apply[T: Algorithm, F[_]: Functor, A, B](
      implicit src: Source[F, A],
      T: Transformation[A, B],
      sink: Sink[B]
    ): Default[T, F, A, B] = {
      val G: Algorithm[T] = implicitly
      val F: Functor[F]   = implicitly
      new Flow[T, A, B] {
        final override type Out = Flow.Out[F, B]
        override def apply(alg: String, payload: String): Out = {
          val b = G.name == alg
          println(s"$alg matches(${G.name}) = $b")
          if (b) Right {
            val in: F[A]       = src(payload)
            val computed: F[B] = F.map(in)(T)
            val r: F[Unit]     = F.map(computed)(sink)
            //r
            computed
          } else Left(())
        }
      }
    }
  }

  trait One
  trait Two
  trait Three

  object allImplicits {
    implicit val a = new Algorithm[One]   { override val name = "one"   }
    implicit val b = new Algorithm[Two]   { override val name = "two"   }
    implicit val c = new Algorithm[Three] { override val name = "three" }

    implicit val src = Source[Option, Int] { line: String ⇒
      Option(line.length)
    }

    implicit val src0 = Source[cats.Id, Int] { line: String ⇒
      line.length
    }

    implicit val map = Transformation { v: Int ⇒
      v * -1
    }

    implicit val consoleSink = Sink[Int] { v: Int ⇒
      println(s"out > $v")
    }

  }

  import implicits._

  def opOne: Flow.Aux[One, Int, Int, Either[Unit, Option[Int]]] = {
    import allImplicits._
    Flow[One, Option, Int, Int]
  }

  def opTwo: Flow.Aux[Two, Int, Int, Either[Unit, Option[Int]]] = {
    import allImplicits._
    Flow[Two, Option, Int, Int]
  }

  def opThree: Flow.Aux[Three, Int, Int, Either[Unit, Option[Int]]] = {
    import allImplicits._
    Flow[Three, Option, Int, Int]
  }

  def opOne0: Flow.Aux[One, Int, Int, Either[Unit, cats.Id[Int]]] = {
    import allImplicits._
    Flow[One, cats.Id, Int, Int]
  }

  def opTwo0: Flow.Aux[Two, Int, Int, Either[Unit, cats.Id[Int]]] = {
    import allImplicits._
    Flow[Two, cats.Id, Int, Int]
  }

  def opThree0: Flow.Aux[Three, Int, Int, Either[Unit, cats.Id[Int]]] = {
    import allImplicits._
    Flow[Three, cats.Id, Int, Int]
  }

  def parseCoP[F[_], T](env: Payload[F, T]): (String, F[T]) =
    env match {
      case Inl(r) ⇒
        (allImplicits.a.name, r)
      case Inr(Inl(r)) ⇒
        (allImplicits.b.name, r)
      case Inr(Inr(Inl(r))) ⇒
        (allImplicits.c.name, r)
      case Inr(Inr(Inr(Inl(_)))) ⇒
        ("unknown", null.asInstanceOf[F[T]])
      case Inr(Inr(Inr(Inr(_)))) ⇒
        ("unknown", null.asInstanceOf[F[T]])
    }

  def main(args: Array[String]): Unit = {
    //val pipeline = opOne0 +: opTwo0 +: opThree0 +: PNil
    val pipeline = opOne +: opTwo +: opThree +: PNil

    val out = pipeline(allImplicits.c.name, "000000")
    val parseOut     = parseCoP(out)
    println(out + " : " + parseOut)
  }
}
