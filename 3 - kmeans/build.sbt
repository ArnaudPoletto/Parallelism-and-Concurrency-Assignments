name := course.value + "-" + assignment.value

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-deprecation")

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % "test"
libraryDependencies ++= assignmentsMap.value.values.flatMap(_.dependencies).toSeq

// include the common dir
commonSourcePackages += "common"

assignmentsMap := {
  val depsCollections = Seq(
    "com.storm-enroute" %% "scalameter-core" % "0.6",
    "com.github.scala-blitz" %% "scala-blitz" % "1.1",
    "org.scala-lang.modules" %% "scala-swing" % "1.0.1",
    "com.storm-enroute" %% "scalameter" % "0.6" % "test"
  )
  val depsSpark = Seq(
    "org.apache.spark" %% "spark-core" % "1.2.1"
  )
  val styleSheetPath = (baseDirectory.value / ".." / ".." / "project" / "scalastyle_config.xml").getPath
}
