package nz.co.jing

import java.nio.charset.CodingErrorAction
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner

import scala.io.{Codec, Source}
import scala.math.sqrt

object RecommendMovies {


  /** Load movies name as Map :  MovieId -> MovieName **/
  def loadMovieNames(movieInfoFile: String): Map[Int, String] ={

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile(movieInfoFile).getLines()

    for(line <- lines) {
      val fields = line.split("::")
      if(fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames

  }

  type MovieRating = (Int, Double)
  /** Parse the Movie Rating line **/  //UserID::MovieID::Rating::Timestamp =>  (userId, (MovieId, Rating))
  def parseRatingLine(line: String) :(Int, MovieRating) = {
    var userId: Int = 0
    var movieId: Int = 0
    var rating: Double = 0

    val fields = line.split("::")
    if (fields.length > 2) {
      userId = fields(0).toInt
      movieId = fields(1).toInt
      rating = fields(2).toDouble
    }
    (userId,(movieId, rating))
  }

  type UserRatingPair = (Int, (MovieRating, MovieRating))  //  (userId, ((MovieId, Rating), (MovieId, Rating)))
  /** Filter the duplicate MovieRating pair, only return the pair which MovieId order ascending **/
  def filterDuplicate(userRating:UserRatingPair):Boolean = {
    val movieId1 = userRating._2._1._1
    val movieId2 = userRating._2._2._1

    return movieId1 < movieId2
  }

  type MoviePair = ((Int, Int), (Double, Double))   // ( (MovieId, MovieId), (Rating, Rating) )
  /** Convert UserRatingPair  to MoviePair **/
  def convertToMoviePair(userRating:UserRatingPair):MoviePair = {
    val movieId1 = userRating._2._1._1
    val movieId2 = userRating._2._2._1

    val rating1 = userRating._2._1._2
    val rating2 = userRating._2._2._2

    return ((movieId1, movieId2), (rating1, rating2))

  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  /** Compute the similarity of the two movies by using Cosine Similarity Algorithm **/
  def computeCosineSimilarity(ratingPairs: RatingPairs):(Double, Int) = {
    var numOfPair: Int = 0
    var sumXX: Double = 0.0
    var sumYY: Double = 0.0
    var sumXY: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sumXX += ratingX * ratingX
      sumYY += ratingY * ratingY
      sumXY += ratingX * ratingY
      numOfPair += 1
    }

    val denominator = sqrt(sumXX) * sqrt(sumYY)
    val numerator:Double = sumXY

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numOfPair)

  }

  /** Our main function **/
  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(this.getClass.getName)

    var movieId: Int = 0
    var ratingFile: String = ""
    var movieInfoFile: String =  ""

    // Parse input args
    if(args.length != 3) {
      log.error(" ERROR : 3 input value are require: MovieId, Rating File, Movie List")
      return
    }
    else {
      movieId = args(0).toInt
      ratingFile = args(1)
      movieInfoFile = args(2)
    }

    println("Input args are : " +  movieId + " " + ratingFile + " " + movieInfoFile )

    val conf = new SparkConf()
    conf.setAppName("RecommendMovies")
    val sc = new SparkContext(conf)
    //val sc = new SparkContext("local", "SimilarMovies") // for local run

    sc.setLogLevel("ERROR")

    log.info("Load movies name.")
    val nameDict = loadMovieNames(movieInfoFile)      //MovieId -> MovieName

    log.info("Load movies rating data")
    val userMovieRating = sc.textFile(ratingFile)

    val userRatingRDD = userMovieRating.map(parseRatingLine).filter(x => (x._1 != 0 && x._2._1 != 0 && x._2._2 != 0))

    val joinedUserRating = userRatingRDD.join(userRatingRDD)    // userId => ((MovieId, Rating), (MovieId, Rating))

    val uniqueJoinUserRating = joinedUserRating.filter(filterDuplicate)

    val moviePairRating = uniqueJoinUserRating.map(convertToMoviePair).partitionBy(new HashPartitioner(100))   // ((MovieId, MovieId), (Rating, Rating))

    val moviePairRatingGroup = moviePairRating.groupByKey() // ((MovieId, MovieId), (Rating, Rating), (Rating, Rating)...)

    val moviePairSimilarity = moviePairRatingGroup.mapValues(computeCosineSimilarity).cache()
    // ((MovieId, MovieId), (Score, Cooccurence))

    //moviePairSimilarity.saveAsTextFile("log/movie-sims")

    // Now, find the similarities of the movie we are interested in
    val scoreThreshold = 0.97
    val cooccurenceThreshold = 1000

    val results = moviePairSimilarity.filter(x => {
      val moviePair = x._1
      val similarity = x._2

      (movieId == moviePair._1 || movieId == moviePair._2) && similarity._1 > scoreThreshold && similarity._2 > cooccurenceThreshold

    }).sortBy(_._2,false).take(10)

    //results.foreach(println)

    println("The top 10 similar movie for " + nameDict(movieId) + " :")

    for(result <- results) {
      val moviePair = result._1
      val similarity = result._2

      var similarMovie = moviePair._1
      if (movieId == moviePair._1) {
        similarMovie = moviePair._2
      }
      println(f"${nameDict(similarMovie)}\t score: ${similarity._1}%.5f counted by ${similarity._2}%2d")
    }
  }


}
