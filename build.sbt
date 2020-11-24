name := "untitled2"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.1.4"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"


dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.7.2",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.2",
  )

}