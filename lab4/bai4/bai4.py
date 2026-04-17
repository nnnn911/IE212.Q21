from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

def main():
    spark = SparkSession.builder.appName("Task4").getOrCreate()

    # Yêu cầu 4: Phân tích số lượng đơn hàng nhóm theo năm, tháng đặt hàng 
    # (Hiển thị theo năm tăng dần, tháng giảm dần)
    
    orders_df = spark.read.csv("Orders.csv", header=True, sep=";", inferSchema=True)

    # Trích xuất Năm và Tháng từ Order_Purchase_Timestamp
    orders_with_date = orders_df.withColumn("Year", year("Order_Purchase_Timestamp")) \
                                .withColumn("Month", month("Order_Purchase_Timestamp"))

    # Nhóm theo Year, Month, và đếm số lượng
    orders_by_date = orders_with_date.groupBy("Year", "Month").count()

    # Sắp xếp theo năm tăng dần, tháng giảm dần
    orders_by_date_sorted = orders_by_date.orderBy(orders_by_date["Year"].asc(), orders_by_date["Month"].desc())

    print("Số lượng đơn hàng theo năm, tháng đặt hàng:")
    orders_by_date_sorted.show()

    spark.stop()

if __name__ == "__main__":
    main()
