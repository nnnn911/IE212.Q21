val moviesRDD = sc.textFile("lab3/movies.txt")
val movieMap = moviesRDD.map(line => {
  val firstComma = line.indexOf(',')
  val lastComma = line.lastIndexOf(',')
  val movieId = line.substring(0, firstComma).trim
  val title = line.substring(firstComma + 1, lastComma).trim
  (movieId, title)
}).collectAsMap()

val ratings1 = sc.textFile("lab3/ratings_1.txt")
val ratings2 = sc.textFile("lab3/ratings_2.txt")
val ratingsRDD = ratings1.union(ratings2)

val movieRatings = ratingsRDD.map(line => { val f = line.split(","); (f(1).trim, (f(2).trim.toDouble, 1)) })
val reduced = movieRatings.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
val avgRatings = reduced.map { case (movieId, (sum, count)) => (movieId, sum / count, count) }

val filtered = avgRatings.filter(_._3 >= 5)
val withTitle = filtered.map { case (movieId, avg, count) => (movieMap.getOrElse(movieId, movieId), avg, count) }
val sorted = withTitle.sortBy(_._2, ascending = false)

val best = sorted.first()

val outputLines = sorted.collect().map { case (title, avg, count) => f"$title: avg=$avg%.2f, count=$count" } ++
  Array("", "=== Phim co diem trung binh cao nhat (>= 5 luot) ===", f"${best._1}: avg=${best._2}%.2f, count=${best._3}")

sc.parallelize(outputLines, 1).saveAsTextFile("lab3/bai1/ket-qua")

println("=== Diem trung binh va so luot danh gia theo phim ===")
sorted.collect().foreach { case (title, avg, count) => println(f"$title: avg=$avg%.2f, count=$count") }
println(s"\n=== Phim co diem trung binh cao nhat (>= 5 luot) ===")
println(f"${best._1}: avg=${best._2}%.2f, count=${best._3}")
