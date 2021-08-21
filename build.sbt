
name := "hashing"
version := "0.0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Dependencies.Compile.all
libraryDependencies ++= Dependencies.Test.all

updateOptions := updateOptions.value.withCachedResolution(true)

resolvers += "Akka Snapshot Repository" at "https://repo.akka.io/snapshots/"
resolvers += Resolver.bintrayRepo("stanch", "maven")

promptTheme := ScalapenosTheme

scalafmtOnCompile := true

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)


// ammonite repl
//test:run
sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue
