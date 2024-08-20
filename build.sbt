organization := "mix"
name := "wm-2"
version := "1.0"

scalaVersion := "2.13.14"
// For Settings/Task reference, see http://www.scala-sbt.org/release/sxr/sbt/Keys.scala.html

lazy val scalaTestVersion = "3.2.19"

libraryDependencies ++= Seq(
  "com.lihaoyi" %% "upickle" % "4.0.0",
  "com.lihaoyi" %% "pprint" % "0.9.0",
  "io.airlift" % "aircompressor" % "0.27",
  "org.scala-lang.modules" %% "scala-xml" % "2.3.0",
  "org.scalatest" %% "scalatest-flatspec" % scalaTestVersion % "test",
  "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.slf4j" % "slf4j-api" % "2.0.16",
  "org.slf4j" % "slf4j-simple" % "2.0.16"
)
