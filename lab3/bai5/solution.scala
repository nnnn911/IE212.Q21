val occupationRDD = sc.textFile("lab3/occupation.txt")
val occupationMap = occupationRDD.map(line => {
  val f = line.split(",")
  (f(0).trim, f(1).trim)
}).collectAsMap()

val usersRDD = sc.textFile("lab3/users.txt")
val userOccupation = usersRDD.map(line => {
  val f = line.split(",")
  val occId = f(3).trim
  (f(0).trim, occupationMap.getOrElse(occId, occId))
}).collectAsMap()

val ratings1 = sc.textFile("lab3/ratings_1.txt")
val ratings2 = sc.textFile("lab3/ratings_2.txt")
val ratingsRDD = ratings1.union(ratings2)

val occRatings = ratingsRDD.map(line => {
  val f = line.split(",")
  val userId = f(0).trim
  val rating = f(2).trim.toDouble
  val occ = userOccupation.getOrElse(userId, "Unknown")
  (occ, (rating, 1))
})

val occAvg = occRatings.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .map { case (occ, (sum, count)) => (occ, sum / count, count) }
val sorted = occAvg.sortBy(_._2, ascending = false)

sorted.map { case (occ, avg, count) => f"$occ: avg=$avg%.2f, count=$count" }
  .coalesce(1).saveAsTextFile("lab3/bai5/ket-qua")

println("=== Trung binh rating va so luot danh gia theo Occupation ===")
sorted.collect().foreach { case (occ, avg, count) => println(f"$occ: avg=$avg%.2f, count=$count") }
