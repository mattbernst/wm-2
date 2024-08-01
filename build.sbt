organization := "mix"
name := "wm-2"
version := "1.0"

scalaVersion := "2.13.14"
// For Settings/Task reference, see http://www.scala-sbt.org/release/sxr/sbt/Keys.scala.html

lazy val scalaTestVersion = "3.2.19"

libraryDependencies ++= Seq(
  "io.airlift" % "aircompressor" % "0.27",
  "org.scalatest" %% "scalatest-freespec" % scalaTestVersion % "test",
  "org.scalatest" %% "scalatest-mustmatchers" % scalaTestVersion % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
)
