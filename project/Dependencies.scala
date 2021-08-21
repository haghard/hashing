import sbt._

object Dependencies {
  val akkaVersion = "2.5.31"

  object Compile {
    //val hasher = "com.roundeights" %% "hasher" % "1.2.0"

    val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion

    val opentracing = "io.opentracing" % "opentracing-api" % "0.21.0"
    val zipkinSender = "io.zipkin.reporter" % "zipkin-sender-okhttp3" % "0.7.0"
    val brave = "io.zipkin.brave" % "brave" % "4.2.0"

    val simulacrum = "com.github.mpilquist" %% "simulacrum" % "0.12.0"

    val shapeless = "com.chuusai" %% "shapeless" % "2.3.3"

    val cats = "org.typelevel" %% "cats-core" % "2.3.1"
    //"org.typelevel" %% "cats" % "0.9.0"

    //val trace = Seq("org.apache.htrace" % "htrace-core4" % "4.3.0-SNAPSHOT", "org.apache.htrace" % "htrace-zipkin" % "4.3.0-SNAPSHOT")

    //val sourceCode = "com.lihaoyi" %% "sourcecode" % "0.1.3"
    //val reftree = "org.stanch" %% "reftree" % "1.0.0"

    val algebird = "com.twitter" %% "algebird-core" % "0.13.0"

    val intervalset = "com.rklaehn" %% "intervalset" % "0.2.0"

    //test:run
    val ammonite = ("com.lihaoyi" % "ammonite" % "2.2.0" % "test").cross(CrossVersion.full)

    //val jumpCH = "testanythinghere" %% "testanythinghere" % "0.1"

    val all = Seq(brave, zipkinSender, opentracing, simulacrum, cats, shapeless,
      //reftree,
      akkaStream, algebird, ammonite)
  }

  object Test {
    //val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    val scalatest = "org.scalatest" %% "scalatest" % "3.1.2" % "test"
    val all = Seq(scalatest)
  }

}
