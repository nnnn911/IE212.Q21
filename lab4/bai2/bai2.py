from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("Task2").getOrCreate()

    # Yêu cầu 2: Thống kê tổng số đơn hàng, số lượng khách hàng và người bán.
    
    orders_df = spark.read.csv("Orders.csv", header=True, sep=";", inferSchema=True)
    customer_df = spark.read.csv("Customer_List.csv", header=True, sep=";", inferSchema=True)
    order_items_df = spark.read.csv("Order_Items.csv", header=True, sep=";", inferSchema=True)

    # Thống kê
    total_orders = orders_df.select("Order_ID").distinct().count()
    
    # số lượng khách hàng chính là tổng số dòng trong bảng Customer_List
    total_customers = customer_df.count()
    
    total_sellers = order_items_df.select("Seller_ID").distinct().count()

    print(f"Tổng số đơn hàng: {total_orders}")
    print(f"Tổng số khách hàng: {total_customers}")
    print(f"Tổng số người bán: {total_sellers}")

    spark.stop()

if __name__ == "__main__":
    main()
