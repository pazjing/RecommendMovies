name := "RecommendMovies"

version := "1.0"

organization := "co.nz.jing"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.apache.spark" % "spark-core_2.11" % "2.1.0" 
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
