import sbt._

object Dependencies {
  val akkaVersion = "2.5.0"

  object Compile {
    //val ckite = "io.ckite" %% "ckite-core" % "0.2.1"
    //val zipkinF = "com.beachape" %% "zipkin-futures" % "0.2.1"
    //val tracing = "com.github.levkhomich" %% "akka-tracing-http" % "0.6.1-SNAPSHOT"

    val hasher = "com.roundeights" %% "hasher" % "1.2.0"
    val akkaActor = "com.typesafe.akka" %% "akka-actor" % akkaVersion

    val opentracing = "io.opentracing" % "opentracing-api" % "0.21.0"
    val zipkinSender = "io.zipkin.reporter" % "zipkin-sender-okhttp3" % "0.7.0"
    val brave = "io.zipkin.brave" % "brave" % "4.2.0"

    val simulacrum = "com.github.mpilquist"    %%   "simulacrum"  %  "0.10.0"

    val all = Seq(akkaActor, hasher, brave, zipkinSender, opentracing, simulacrum)
  }

  object Test {
    val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    val scalatest = "org.scalatest" %% "scalatest" % "3.0.3" % "test"
    val all = Seq(akkaTestkit, scalatest)
  }

}
