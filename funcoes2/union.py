from pyspark.sql import SparkSession, functions as f

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    df_cars = spark.read.json('../cars.json').orderBy('brand')
    df_cars.show()


    df_cars_audi = df_cars.filter('brand = "audi"')
    df_cars_bmw = df_cars.filter('brand = "bmw"')



    df_cars_audi.union(df_cars_bmw).show()  
