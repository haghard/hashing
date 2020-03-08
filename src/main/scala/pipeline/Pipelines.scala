package pipeline

//https://github.com/dreadedsoftware/talks/blob/master/tls2017/Pipelines.scala
object Pipelines {
  import cats.Functor
  import cats.instances.option._

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

  //For each Pipeline there is a Filter with a segment
  trait Filter[-T] {
    def segment: String
  }

  sealed trait Pipeline[-T, A, B] {
    type Out
    def feed(input: String): Out
  }

  //Input -> Computation -> Output
  object Pipeline {
    def apply[T: Filter, F[_]: cats.Functor, In, Out](
      implicit read: Source[F, In],
      computation: Transformation[In, Out],
      write: Sink[Out]
    ) = {
      val F: cats.Functor[F] = implicitly[cats.Functor[F]]
      val f: Filter[T]       = implicitly[Filter[T]]
      new Pipeline[T, In, Out] {
        type Out = Either[String, F[Unit]]
        override def feed(input: String): Out = {
          val b = input.contains(f.segment)
          println(s"input: $input - filter: ${f.segment} res: $b")
          if (b) Right {
            val in       = read(input)
            val computed = F.map(in)(computation)
            F.map(computed)(write)
          }
          else Left(input)
        }
      }
    }
  }

  trait One

  trait Two

  trait Three

  implicit val a = new Filter[One] {
    override def segment: String = "one"
  }

  implicit val b = new Filter[Two] {
    override def segment: String = "two"
  }

  implicit val c = new Filter[Three] {
    override def segment: String = "three"
  }

  implicit val src  = Source[Option, Int]((line: String) ⇒ Option(line.length))
  implicit val map  = Transformation((v: Int) ⇒ v * -1)
  implicit val sink = Sink[Int]((v: Int) ⇒ println(s"out > $v"))

  val pipelines =
    Pipeline[One, Option, Int, Int]
      .feed("three")
      .fold(
        Pipeline[Two, Option, Int, Int]
          .feed(_)
          .fold(Pipeline[Three, Option, Int, Int].feed(_).fold(Left(_), Right(_)), Right(_)),
        Right(_)
      )

  /*
  def second(in: String) =
    Pipeline[Two, Option, Int, Int].feed(in).fold(third(_), Right(_))

  def third(in: String) =
    Pipeline[Three, Option, Int, Int].feed(in).fold(Left(_), Right(_))
   */

  pipelines
}
