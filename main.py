from pyspark.sql import SparkSession
import findspark

findspark.init()


def method():
    spark = SparkSession.builder.appName("testing").getOrCreate()
    data = [
        ('1', 'Beans'),
        ('2', 'Bread'),
        ('3', 'Butter'),
        ('4', 'Meat'),
        ('5', 'Candies'),
        ('6', 'Fish'),
        ('7', 'Cheese'),
    ]
    column = ['product_id', 'product_name']

    df = spark.createDataFrame(data, column)

    data2 = [
        ('1', 'Protein_products'),
        ('2', 'Cereals'),
        ('3', 'Milk_products'),
    ]
    column = ['category_id', 'category_name']
    df2 = spark.createDataFrame(data2, column)

    data3 = [
        ('1', '1'),
        ('1', '2'),
        ('2', '2'),
        ('3', '3'),
        ('4', '1'),
        ('5', 'null'),
        ('6', '1'),
        ('7', '3')
    ]
    column = ['product_id', 'category_id']
    df3 = spark.createDataFrame(data3, column)
    df3.join(df, df3["product_id"] == df["product_id"], "outer").drop('product_id') \
        .join(df2, df3["category_id"] == df2["category_id"], "outer").drop('category_id').show()


method()
