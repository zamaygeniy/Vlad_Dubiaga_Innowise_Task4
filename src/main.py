from pyspark.sql import  SparkSession

from src import utils
import queries

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("test")\
        .config("spark.jars", "postgresql-42.4.0.jar")\
        .master("local[*]")\
        .getOrCreate()

    password = input("input password to db: ")
    utils.setup_dataframes_with_postgres(password, spark)

    #1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.
    spark.sql(queries.NUMBER_OF_FILMS_IN_EACH_CATEGORY).show()

    #2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
    spark.sql(queries.TOP_10_ACTORS_BY_RENT_NUMBERS).show()

    #3. Вывести категорию фильмов, на которую потратили больше всего денег.
    spark.sql(queries.TOP_CATEGORY_BY_MONEY).show()

    #4. Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
    spark.sql(queries.FILM_TITLES_WHICH_ARE_NOT_IN_INVENTORY).show()

    #5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
    spark.sql(queries.TOP_3_ACTORS_IN_CHILDREN_MOVIES).show()

    #6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
    spark.sql(queries.CITIES_BY_ACTIVE_AND_INACTIVE_CUSTOMERS).show()

    #7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
    spark.sql(queries.TOP_CATEGORY_BY_RENT_LENGTH).show()

