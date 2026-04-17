from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def main():
    spark = SparkSession.builder.appName("Task5").getOrCreate()

    # Yêu cầu 5: Thống kê điểm đánh giá trung bình, số lượng đánh giá theo từng mức (ví dụ: 1 đến 5).
    # Cần xử lý các giá trị ngoại lệ và NULL trong cột Review_Score.
    
    order_reviews_df = spark.read.csv("Order_Reviews.csv", header=True, sep=";", inferSchema=True)

    # Đảm bảo Review_Score là kiểu số và lớn hơn 0, nhỏ hơn bằng 5
    # Lọc NULL và các giá trị không phải là số hợp lệ
    cleaned_reviews = order_reviews_df.filter(col("Review_Score").isNotNull())
    
    # Chỉ giữ lại các hàng mà Review_Score là một chữ số từ 1 đến 5
    cleaned_reviews = cleaned_reviews.filter(col("Review_Score").rlike("^[1-5]$"))
    
    # ép kiểu và đảm bảo range
    cleaned_reviews = cleaned_reviews.withColumn("Review_Score", col("Review_Score").cast("int"))

    # Tính điểm đánh giá trung bình
    avg_row = cleaned_reviews.select(avg("Review_Score")).collect()
    if avg_row and avg_row[0][0] is not None:
        avg_score = avg_row[0][0]
        print(f"Điểm đánh giá trung bình: {avg_score:.2f}")
    else:
        print("Không có điểm đánh giá trung bình hợp lệ.")

    # Thống kê số lượng đánh giá theo từng mức
    reviews_by_score = cleaned_reviews.groupBy("Review_Score").count().orderBy("Review_Score")
    print("Số lượng đánh giá theo mỗi mức điểm:")
    reviews_by_score.show()

    spark.stop()

if __name__ == "__main__":
    main()
