def ageGroup(age: Int): String = age match {
  case a if a < 18 => "Under 18"
  case a if a <= 24 => "18-24"
  case a if a <= 34 => "25-34"
  case a if a <= 44 => "35-44"
  case a if a <= 54 => "45-54"
  case _ => "55+"
}

val usersRDD = sc.textFile("lab3/users.txt")
val userAgePair = usersRDD.map(line => {
  val f = line.split(",")
  (f(0).trim, ageGroup(f(2).trim.toInt))
})

val moviesRDD = sc.textFile("lab3/movies.txt")
val movieTitleMap = moviesRDD.map(line => {
  val firstComma = line.indexOf(',')
  val lastComma = line.lastIndexOf(',')
  val movieId = line.substring(0, firstComma).trim
  val title = line.substring(firstComma + 1, lastComma).trim
  (movieId, title)
}).collectAsMap()

val ratings1 = sc.textFile("lab3/ratings_1.txt")
val ratings2 = sc.textFile("lab3/ratings_2.txt")
val ratingsRDD = ratings1.union(ratings2)

val ratingsUserPair = ratingsRDD.map(line => {
  val f = line.split(",")
  val userId = f(0).trim
  val movieId = f(1).trim
  val rating = f(2).trim.toDouble
  (userId, (movieId, rating))
})

val ageMovieRatings = ratingsUserPair.join(userAgePair).map {
  case (userId, ((movieId, rating), ag)) =>
    ((movieId, ag), (rating, 1))
}

val avgByMovieAge = ageMovieRatings.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .map { case ((movieId, ag), (sum, count)) =>
    (movieTitleMap.getOrElse(movieId, movieId), ag, sum / count, count)
  }
val sorted = avgByMovieAge.sortBy(r => (r._1, r._2))

sorted.map { case (title, ag, avg, count) => f"$title | $ag: avg=$avg%.2f, count=$count" }
  .coalesce(1).saveAsTextFile("lab3/bai4/ket-qua")

println("=== Diem trung binh theo phim va nhom tuoi ===")
sorted.collect().foreach { case (title, ag, avg, count) =>
  println(f"$title | $ag: avg=$avg%.2f, count=$count")
}
