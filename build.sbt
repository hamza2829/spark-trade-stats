ThisBuild / scalaVersion := "2.13.16"
ThisBuild / organization := "io.github.hamza2829"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-trade-stats",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.7",
      // Pure Java local file access on Windows, without native filesystem binaries.
      "com.globalmentor" % "hadoop-bare-naked-local-fs" % "0.1.0",
      "com.typesafe" % "config" % "1.4.3",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Compile / mainClass := Some("sparktradestats.TradeJob"),
    Compile / run / fork := true,
    Test / fork := true,
    Test / parallelExecution := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint"),
    // Spark's Java 17 module access options, normally supplied by spark-submit.
    javaOptions ++= Seq("-Xmx2G", "-Duser.timezone=UTC") ++ Seq(
      "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
      "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
      "jdk.internal.ref", "sun.nio.ch", "sun.nio.cs", "sun.security.action",
      "sun.util.calendar"
    ).map(p => s"--add-opens=java.base/$p=ALL-UNNAMED") ++ Seq(
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "-Djdk.reflect.useDirectMethodHandle=false"
    )
  )
