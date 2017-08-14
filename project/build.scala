import sbt._
import Keys._

object MacroBuild extends Build {
  lazy val main = Project("main", file(".")) dependsOn(macroSub)
  lazy val macroSub = Project("macro", file("macro")) settings(
    libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value
  )
}
