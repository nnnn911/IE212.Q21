val usersRDD = sc.textFile("lab3/users.txt")
val userGenderPair = usersRDD.map(line => {
  val f = line.split(",")
  (f(0).trim, f(1).trim)
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

val genderMovieRatings = ratingsUserPair.join(userGenderPair).map {
  case (userId, ((movieId, rating), gender)) =>
    ((movieId, gender), (rating, 1))
}

val avgByMovieGender = genderMovieRatings.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .map { case ((movieId, gender), (sum, count)) =>
    (movieTitleMap.getOrElse(movieId, movieId), gender, sum / count, count)
  }
val sorted = avgByMovieGender.sortBy(r => (r._1, r._2))

sorted.map { case (title, gender, avg, count) => f"$title | $gender: avg=$avg%.2f, count=$count" }
  .coalesce(1).saveAsTextFile("lab3/bai3/ket-qua")

println("=== Diem trung binh theo phim va gioi tinh ===")
sorted.collect().foreach { case (title, gender, avg, count) =>
  println(f"$title | $gender: avg=$avg%.2f, count=$count")
}
