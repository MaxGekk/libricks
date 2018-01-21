name := "libricks"

version := "0.2"

scalaVersion := "2.12.4"

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.4",
  "org.json4s" %% "json4s-jackson" % "3.5.3",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)

bintrayPackageLabels := Seq("databricks", "rest", "dbfs", "scala")