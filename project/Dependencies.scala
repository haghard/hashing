import sbt._

object Dependencies {
  val akkaVersion = "2.5.2"

  object Compile {
    val hasher = "com.roundeights" %% "hasher" % "1.2.0"

    val akkaActor = "com.typesafe.akka" %% "akka-actor" % akkaVersion
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion

    val opentracing = "io.opentracing" % "opentracing-api" % "0.21.0"
    val zipkinSender = "io.zipkin.reporter" % "zipkin-sender-okhttp3" % "0.7.0"
    val brave = "io.zipkin.brave" % "brave" % "4.2.0"

    val simulacrum = "com.github.mpilquist" %% "simulacrum" % "0.10.0"

    val shapeless = "com.chuusai" %% "shapeless" % "2.3.2"

    val cats = "org.typelevel" %% "cats" % "0.9.0"

    //val trace = Seq("org.apache.htrace" % "htrace-core4" % "4.3.0-SNAPSHOT", "org.apache.htrace" % "htrace-zipkin" % "4.3.0-SNAPSHOT")

    //val sourceCode = "com.lihaoyi" %% "sourcecode" % "0.1.3"
    val reftree = "org.stanch" %% "reftree" % "1.0.0"


    val all = Seq(akkaActor, hasher, brave, zipkinSender, opentracing, simulacrum, cats, shapeless,
      reftree, akkaStream)
  }

  object Test {
    //val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    val scalatest = "org.scalatest" %% "scalatest" % "3.0.3" % "test"
    val all = Seq(scalatest)
  }

}
