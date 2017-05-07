import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

name := "hashing"
version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Dependencies.Compile.all
libraryDependencies ++= Dependencies.Test.all

enablePlugins(JavaAppPackaging)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

updateOptions := updateOptions.value.withCachedResolution(true)

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
resolvers += Resolver.bintrayRepo("stanch", "maven")

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveDanglingCloseParenthesis, true)
  .setPreference(RewriteArrowSymbols, true)

