# recommendMovies
Learning Scala, Spark and SBT


# How to run
Clone the repository: git clone https://github.com/pazjing/recommendMovies.git

Execute the sbt command ```sbt assembly``

RecommendMovies-assembly-1.0.jar is generated under target/scala-2.11/

Submit the spark job by 
``` spark-submit RecommendMovies-assembly-1.0.jar <movieId> <ratingsFile> <movieName>```
e.g. 
spark-submit RecommendMovies-assembly-1.0.jar 50 ratings.dat movies.dat
__Note: Only use small dataset ratings2.dat and movies2.dat first. Only use the full dataset in cluster like EMR__



