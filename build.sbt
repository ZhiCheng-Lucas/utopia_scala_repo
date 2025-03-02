name := "camera-detection-analytics"
organization := "com.utopia"
version := "1.0.0"
scalaVersion := "2.13.16"
val sparkVersion = "3.5.4"

// ScalaStyle configuration
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value

lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(Test).toTask("").value
(Test / test) := ((Test / test) dependsOn testScalastyle).value

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Assembly plugin for creating fat JAR
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// Exclude Spark libraries from the fat JAR since they'll be provided by the cluster
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp filter { f =>
    f.data.getName.startsWith("spark-core") ||
    f.data.getName.startsWith("spark-sql") ||
    f.data.getName.startsWith("scala-library")
  }
}

// Merge strategy for handling conflicting files during assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}

// Set the main class for the JAR
Compile / mainClass := Some("com.utopia.analytics.CameraDetectionAnalytics")
