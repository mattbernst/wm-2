organization := "wiki"
name := "wm-2"
version := "1.0"

scalaVersion := "2.13.16"
// For Settings/Task reference, see http://www.scala-sbt.org/release/sxr/sbt/Keys.scala.html

lazy val scalaTestVersion = "3.2.19"

libraryDependencies ++= Seq(
  "com.github.blemale" %% "scaffeine" % "5.3.0",
  "com.lihaoyi" %% "upickle" % "4.0.0",
  "com.lihaoyi" %% "pprint" % "0.9.0",
  "io.airlift" % "aircompressor" % "0.27",
  "org.apache.opennlp" % "opennlp-tools" % "2.4.0",
  "org.scala-lang.modules" %% "scala-xml" % "2.3.0",
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.2.0",
  "org.scalatest" %% "scalatest-flatspec" % scalaTestVersion % "test",
  "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.slf4j" % "slf4j-api" % "2.0.16",
  "org.slf4j" % "slf4j-simple" % "2.0.16",
  "org.scalikejdbc" %% "scalikejdbc" % "4.3.1",
  "org.sweble.wikitext" % "swc-parser-lazy" % "3.1.9",
  // This is required for Sweble on Java versions above 8
  "javax.xml.bind" % "jaxb-api" % "2.3.1",
  "org.xerial" % "sqlite-jdbc" % "3.46.1.0"
)

assembly / assemblyMergeStrategy := {
  // This strategy is required to assembly a jar with Sweble included
  case PathList("org", "xmlpull", xs @ _*) => MergeStrategy.first
  case "module-info.class" => MergeStrategy.discard
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
  case x =>
    (assembly / assemblyMergeStrategy).value.apply(x)
}
