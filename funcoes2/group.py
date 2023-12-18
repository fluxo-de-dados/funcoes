from pyspark.sql import SparkSession, functions as f

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    df_cars = spark.read.json('../cars.json').orderBy('brand')
    df_cars.show()
    
    df_cars.groupBy('brand').count().show()

    df_cars.groupBy('brand').agg(
        f.count('Horsepower').alias('count'),
        f.maxt('Horsepower').alias('max'),
        f.min('Horsepower').alias('min'),
        f.avg('Horsepower').alias('avg'),
    ).show()


