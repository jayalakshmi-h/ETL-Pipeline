name := "adjust-assignment"

version := "0.1"

scalaVersion := "2.13.8"

val sparkVersion = "3.2.0"

libraryDependencies += "org.postgresql" % "postgresql" % "42.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

val AkkaVersion = "2.6.14"
libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "3.0.4",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)
