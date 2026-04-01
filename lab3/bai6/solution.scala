import java.time.{Instant, ZoneId, LocalDate}

def timestampToYear(ts: Long): Int = {
  val date: LocalDate = Instant.ofEpochSecond(ts).atZone(ZoneId.systemDefault()).toLocalDate()
  date.getYear()
}

val ratings1 = sc.textFile("lab3/ratings_1.txt")
val ratings2 = sc.textFile("lab3/ratings_2.txt")
val ratingsRDD = ratings1.union(ratings2)

val yearRatings = ratingsRDD.map(line => {
  val f = line.split(",")
  val rating = f(2).trim.toDouble
  val year = timestampToYear(f(3).trim.toLong)
  (year, (rating, 1))
})

val yearAvg = yearRatings.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .map { case (year, (sum, count)) => (year, sum / count, count) }
val sorted = yearAvg.sortBy(_._1)

sorted.map { case (year, avg, count) => f"$year: avg=$avg%.2f, count=$count" }
  .coalesce(1).saveAsTextFile("lab3/bai6/ket-qua")

println("=== Tong so luot danh gia va diem trung binh theo nam ===")
sorted.collect().foreach { case (year, avg, count) => println(f"$year: avg=$avg%.2f, count=$count") }
