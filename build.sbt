name := "libricks"

version := "0.6"

//scalaVersion := "2.12.4"
scalaVersion := "2.10.7"

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.5",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)

bintrayPackageLabels := Seq("databricks", "rest", "dbfs", "scala")

enablePlugins(AssemblyPlugin)

mainClass in assembly := Some("com.databricks.Libricks")
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"