name := "libricks"

version := "0.1"

scalaVersion := "2.12.4"

licenses += ("Apache-2.0", url("https://opensource.org/licenses/apache-2.0"))

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.4",
  "org.json4s" %% "json4s-jackson" % "3.5.3",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)