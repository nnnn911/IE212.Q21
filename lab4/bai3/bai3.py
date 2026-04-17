from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Task3").getOrCreate()

    # Yêu cầu 3: Phân tích số lượng đơn hàng theo quốc gia, sắp xếp theo thứ tự giảm dần.
    
    orders_df = spark.read.csv("Orders.csv", header=True, sep=";", inferSchema=True)
    customer_df = spark.read.csv("Customer_List.csv", header=True, sep=";", inferSchema=True)

    # Join orders and customer
    orders_with_country = orders_df.join(customer_df, "Customer_Trx_ID", "inner")

    # Group by Customer_Country, count and sort
    orders_by_country = orders_with_country.groupBy("Customer_Country").count()
    orders_by_country_sorted = orders_by_country.orderBy("count", ascending=False)

    print("Số lượng đơn hàng phân theo quốc gia:")
    orders_by_country_sorted.show(50)

    spark.stop()

if __name__ == "__main__":
    main()
