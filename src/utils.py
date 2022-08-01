from pyspark.sql import SparkSession


def setup_dataframes_with_postgres(password: str, spark: SparkSession):
    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.address") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("address")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.customer") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .customer_df.createOrReplaceTempView("customer")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.city") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("city")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.film") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("film")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.category") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("category")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.film_category") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("film_category")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.rental") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("rental")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.inventory") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("inventory")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.actor") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("actor")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.film_actor") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("film_actor")

    spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/pagila") \
        .option("dbtable", "public.payment") \
        .option("user", "postgres") \
        .option("password", password) \
        .load() \
        .createOrReplaceTempView("payment")
