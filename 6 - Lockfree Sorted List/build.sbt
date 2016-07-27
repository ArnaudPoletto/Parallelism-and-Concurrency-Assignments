name := "concpar-assignments"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "junit" % "junit" % "4.8.1" % "test"
)

javaOptions in Test += "-Xmx4G"

fork in Test := true
