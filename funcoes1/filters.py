from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    df_cars = spark.read.json('../cars.json')
    
    df_cars_eu = df_cars.filter('Origin = "Europe" ')
    df_cars_us = df_cars.filter('Origin = "USA" ')

    # df_cars_us.write.csv('cars_us')
    # df_cars_eu.write.csv('cars_eu')

    df_cars.filter('Acceleration > 14.2').show()

    