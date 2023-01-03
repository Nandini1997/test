from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import split,col,countDistinct, sum

def sparkSession():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark

def trasactiondf(spark):
    transactionSchema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("userid", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("product_description", StringType(), True)
    ])

    transaction_df = spark.read.option("header", True).schema(transactionSchema).csv(r"\Users\NandiniSrinivas\PycharmProjects\pythonProject\Resource\transaction.csv")
    return transaction_df

def userdf(spark):
    userSchema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("emailid", StringType(), True),
        StructField("nativelanguage", StringType(), True),
        StructField("location ", StringType(), True)
    ])
    user_df = spark.read.option("header", True).schema(userSchema).csv(r"C:\Users\NandiniSrinivas\PycharmProjects\pythonProject\Resource\user.csv")
    return user_df

def joining_trans_user(transaction_df, user_df):
    transaction_user_df = transaction_df.join(user_df, transaction_df.userid == user_df.user_id, "inner")
    return transaction_user_df

def countDistinctLocation(transaction_user_df,location):
    count_of_location = transaction_user_df.select(countDistinct(location).alias("Distinct_location_count"))
    return count_of_location

def prod_by_users(trans_user_df,userid,prod_desc):
    products_users = trans_user_df.select(col(userid), col(prod_desc))
    return products_users

def sum_spending_by_prod(transaction_user_df,userid, prodid, price):
    spending_each_price = transaction_user_df.groupBy("userid", "product_id").agg(sum("price"))
    return spending_each_price

