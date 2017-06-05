package pipeline

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

//pipeline.AkkaStreamsPipeline.run
//https://softwaremill.com/shapeless-hlist-akka-stream/
object AkkaStreamsPipeline {

  import akka.stream.FlowShape
  import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge }
  import shapeless._
  import shapeless.ops.hlist._
  import types.{ Data, ProcessingFlow }

  object types {
    type Data = String
    type ProcessingFlow[_] = Flow[Data, Data, akka.NotUsed]
  }

  import shapeless.PolyDefns.~>

  object toFlow extends (ParserStageDef ~> ProcessingFlow) {
    override def apply[T](f: ParserStageDef[T]) = {
      //val A: cats.Applicative[Option[T]] = implicitly[cats.Applicative[Option[T]]]
      //val F: cats.Functor[Option[T]] = implicitly[cats.Functor[Option[T]]]
      Flow[Data].map((f.parser andThen f.processor))
    }
  }

  trait ParserStageDef[T] {
    def parser: Data ⇒ T
    def processor: T ⇒ Data
  }

  def buildGraph[T <: HList, Mapped <: HList](stages: T)(implicit m: Mapper.Aux[toFlow.type, T, Mapped], t: ToTraversable.Aux[Mapped, List, ProcessingFlow[_]]) = {
    val specsSize = stages.runtimeLength
    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[Data](specsSize))
      val merge = builder.add(Merge[Data](specsSize))
      stages.map(toFlow).toList.foreach { flow ⇒
        broadcast ~> flow ~> merge
      }

      FlowShape(broadcast.in, merge.out)
    }
  }

  def run = {
    val decorator: ParserStageDef[String] = new ParserStageDef[String] {
      override def parser = identity
      override def processor = i ⇒ s"{ $i }"
    }

    val incrementer: ParserStageDef[Int] = new ParserStageDef[Int] {
      override def parser = _.toInt
      override def processor = i ⇒ (i + 1).toString
    }

    val stages = decorator :: incrementer :: HNil
    val metaFlow = buildGraph(stages)
    implicit val system = ActorSystem("flow")
    implicit val m = ActorMaterializer.create(system)
    implicit val ec = system.dispatcher
    Source(List("1", "2", "3", "4")).via(metaFlow).runWith(Sink.seq)
      .foreach(r ⇒ println(r.mkString(",")))

    Await.result(system.terminate(), Duration.Inf)
  }
}