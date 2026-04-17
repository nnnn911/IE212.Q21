from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, round, sum

def main():
    spark = SparkSession.builder.appName("Task6").getOrCreate()

    # Yêu cầu 6: Tính doanh thu (giá sản phẩm + phí vận chuyển) trong năm 2024 và nhóm theo danh mục sản phẩm
    
    orders_df = spark.read.csv("Orders.csv", header=True, sep=";", inferSchema=True)
    order_items_df = spark.read.csv("Order_Items.csv", header=True, sep=";", inferSchema=True)
    products_df = spark.read.csv("Products.csv", header=True, sep=";", inferSchema=True)

    # Lọc các đơn hàng trong năm 2024
    orders_2024 = orders_df.filter(year("Order_Purchase_Timestamp") == 2024)

    # Kết hợp các bảng Orders, Order_Items, và Products
    # Orders JOIN Order_Items
    joined_df = orders_2024.join(order_items_df, "Order_ID", "inner")

    # Tiếp tục JOIN với bảng Products
    final_df = joined_df.join(products_df, "Product_ID", "inner")

    # Tính doanh thu theo từng dòng sản phẩm
    revenue_df = final_df.withColumn("Total_Price", col("Price") + col("Freight_Value"))

    # Nhóm theo danh mục sản phẩm và tính tổng doanh thu
    revenue_by_category = revenue_df.groupBy("Product_Category_Name").agg(
        round(sum("Total_Price"), 2).alias("Total_Revenue")
    ).orderBy(col("Total_Revenue").desc())

    print("Doanh thu trong năm 2024 theo từng danh mục sản phẩm:")
    revenue_by_category.show(50)

    spark.stop()

if __name__ == "__main__":
    main()
