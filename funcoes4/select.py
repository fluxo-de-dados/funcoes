from pyspark.sql import SparkSession, functions as f


if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()

    data = [
            {'code':1, 'rate':5, 'active':True,'comment': 'I like that!'},
            {'code':2, 'rate':4, 'active':True,'comment': 'I loved, I will share with my team'},
            {'code':3, 'rate':3, 'active':False,'comment': 'I liked, but I think it could have more features.'}
    ]

    df = spark.createDataFrame(data=data)

    df.show()

    df.select('code','rate',f.concat('code','rate')).show()

    df.selectExpr('code','rate','concat(code,rate)').show()
    
    df.selectExpr('comment','rate','substring(comment,1,10)').show()



    spark.stop()