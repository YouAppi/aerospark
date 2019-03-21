organization := "youappi"
name         := "aerospike-spark"
version      := "1.0." + util.Properties.propOrElse("buildNumber", "0-SNAPSHOT")

credentials += Credentials("Sonatype Nexus Repository Manager", "buildint.youappi.com", "ya_deploy", util.Properties.propOrEmpty("ya_deployer"))

publishTo := {
  val nexus = "https://buildint.youappi.com:8082/"
  Some("maven-releases"  at nexus + "nexus/repository/maven-releases/")
}

crossScalaVersions := Seq("2.10.6", "2.11.12", "2.12.0")

scalaVersion := "2.11.12"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

parallelExecution in test := false

val sparkVer = "2.3.0"
libraryDependencies ++= Seq(
	"org.apache.spark"				%% "spark-core"				% sparkVer % Provided,
	"org.apache.spark" 				%% "spark-sql"				% sparkVer % Provided,
  "com.twitter"					    % "util-core_2.11" 			% "6.34.0",
  "com.aerospike"					%  "aerospike-helper-java"	% "1.2.3",
	"com.aerospike"					%  "aerospike-client"	% 	"4.1.5" % Provided,
	"com.typesafe.scala-logging"	%% "scala-logging-slf4j"	% "2.1.2",
	"org.scalatest"					%% "scalatest"				% "2.2.1" % Test,
	"com.github.docker-java" 		% "docker-java" 			% "3.0.8" % Test,
	"joda-time"						% "joda-time"				% "2.9.9" % Test,
	"org.testcontainers"			% "testcontainers" 			% "1.2.0" % Test
)

resolvers ++= Seq("Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository")
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

cancelable in Global := true

assemblyMergeStrategy in assembly := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", "maven","com.aerospike","aerospike-client", "pom.properties") =>
    MergeStrategy.discard
  case PathList("META-INF", "maven","com.aerospike","aerospike-client", "pom.xml") =>
    MergeStrategy.discard
  case PathList("META-INF", "maven","org.slf4j","slf4j-api", "pom.xml") =>
    MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "pom.properties" =>
    MergeStrategy.discard
  case PathList("META-INF", xs @ _*) =>
    xs.map(_.toLowerCase) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: _) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: _ =>
        MergeStrategy.discard
      case "services" :: _ =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case _ => MergeStrategy.first
}
