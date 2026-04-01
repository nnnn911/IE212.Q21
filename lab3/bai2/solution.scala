val moviesRDD = sc.textFile("lab3/movies.txt")
val movieGenres = moviesRDD.map(line => {
  val firstComma = line.indexOf(',')
  val lastComma = line.lastIndexOf(',')
  val movieId = line.substring(0, firstComma).trim
  val genres = line.substring(lastComma + 1).trim.split("\\|").toList
  (movieId, genres)
}).collectAsMap()

val ratings1 = sc.textFile("lab3/ratings_1.txt")
val ratings2 = sc.textFile("lab3/ratings_2.txt")
val ratingsRDD = ratings1.union(ratings2)

val genreRatings = ratingsRDD.flatMap(line => {
  val f = line.split(",")
  val movieId = f(1).trim
  val rating = f(2).trim.toDouble
  movieGenres.getOrElse(movieId, List()).map(genre => (genre, (rating, 1)))
})

val genreAvg = genreRatings.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .map { case (genre, (sum, count)) => (genre, sum / count) }
val sorted = genreAvg.sortBy(_._2, ascending = false)

sorted.map { case (genre, avg) => f"$genre: avg=$avg%.2f" }.coalesce(1).saveAsTextFile("lab3/bai2/ket-qua")

println("=== Diem trung binh theo the loai phim ===")
sorted.collect().foreach { case (genre, avg) => println(f"$genre: avg=$avg%.2f") }
