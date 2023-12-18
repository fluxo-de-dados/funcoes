from pyspark.sql import SparkSession, functions as f


if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()

    df_usuarios = spark.read.option('header',True).csv('../datasources/usuarios.csv')
    df_eventos = spark.read.option('header',True).csv('../datasources/eventos.csv')


    # df_usuarios.show()
    # df_eventos.show()

    df_usuarios.join(df_eventos,df_usuarios.id == df_eventos.id_usuario,'inner').show()
    df_usuarios.join(df_eventos,df_usuarios.id == df_eventos.id_usuario,'left').show()
    df_usuarios.join(df_eventos,df_usuarios.id == df_eventos.id_usuario,'right').show()
    df_usuarios.join(df_eventos,df_usuarios.id == df_eventos.id_usuario,'outer').show()


    # df_usuarios.join(df_eventos,df_usuarios.id == df_eventos.id_usuario,'outer').drop('evento').show()

    df_usuarios.join(df_eventos,df_usuarios.id == df_eventos.id_usuario,'outer').withColumn('nome_evento', f.concat_ws('|','nome','evento')).show(truncate=False)