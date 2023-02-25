name := "sample-job"
version := "1.0"
scalaVersion := "2.12.17"
//scalaVersion := "3.2.2"

//Last version of spark (in Feb 23)
//val sparkVersion = "3.3.0"
val sparkVersion = "3.3.1"
val slf4jVersion = "1.7.16"

// Note the dependencies are provided
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion //% "provided"
libraryDependencies += "org.slf4j" % "slf4j-api" % slf4jVersion
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % slf4jVersion
// Do not include Scala in the assembled JAR
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// META-INF discarding for the FAT JAR
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val spark_run = taskKey[Unit]("Builds the assembly and ships it to the Spark Cluster")
//spark_run := {
 // ("spark_submit " + assembly.value) !
//}
