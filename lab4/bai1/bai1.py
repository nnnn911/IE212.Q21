from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Task1").getOrCreate()

    # Yêu cầu 1: Đọc dữ liệu từ các file csv, sử dụng tự suy ra kiểu dữ liệu cho mỗi cột.
    
    customer_df = spark.read.csv("Customer_List.csv", header=True, sep=";", inferSchema=True)
    order_items_df = spark.read.csv("Order_Items.csv", header=True, sep=";", inferSchema=True)
    order_reviews_df = spark.read.csv("Order_Reviews.csv", header=True, sep=";", inferSchema=True)
    orders_df = spark.read.csv("Orders.csv", header=True, sep=";", inferSchema=True)
    products_df = spark.read.csv("Products.csv", header=True, sep=";", inferSchema=True)

    print("--- Customer_List Schema ---")
    customer_df.printSchema()
    
    print("--- Order_Items Schema ---")
    order_items_df.printSchema()
    
    print("--- Order_Reviews Schema ---")
    order_reviews_df.printSchema()
    
    print("--- Orders Schema ---")
    orders_df.printSchema()
    
    print("--- Products Schema ---")
    products_df.printSchema()

    spark.stop()

if __name__ == "__main__":
    main()
