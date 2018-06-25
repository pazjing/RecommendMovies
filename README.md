# RecommendMovies
Learning Scala, Spark and SBT


# How to run
Clone the repository: git clone https://github.com/pazjing/recommendMovies.git

Execute the sbt command 

```
sbt assembly
```

RecommendMovies-assembly-1.0.jar is generated under **target/scala-2.11/**

Submit the spark job by spark-submit RecommendMovies-assembly-1.0.jar movieId ratingsFile movieName

e.g. 
```
spark-submit RecommendMovies-assembly-1.0.jar 260 ratings.dat movies.dat

spark-submit RecommendMovies-assembly-1.0.jar 260 s3n://temp-bucket123/ratings.dat  movies.dat
```
The results example:
```
The top 10 similar movie for Star Wars: Episode IV - A New Hope (1977) :
Star Wars: Episode V - The Empire Strikes Back (1980)	 score: 0.98979 counted by 2355
Raiders of the Lost Ark (1981)	 score: 0.98555 counted by 1972
Star Wars: Episode VI - Return of the Jedi (1983)	 score: 0.98412 counted by 2113
Indiana Jones and the Last Crusade (1989)	 score: 0.97744 counted by 1397
Shawshank Redemption, The (1994)	 score: 0.97683 counted by 1412
Usual Suspects, The (1995)	 score: 0.97669 counted by 1194
Godfather, The (1972)	 score: 0.97593 counted by 1583
Sixth Sense, The (1999)	 score: 0.97469 counted by 1480
Schindler's List (1993)	 score: 0.97468 counted by 1422
Terminator, The (1984)	 score: 0.97458 counted by 1746
```

__Note: Only use small dataset ratings2.dat and movies2.dat first. Then use the full dataset in cluster like EMR__



