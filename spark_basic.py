# convert the data to the lower case
    from pyspark.sql import functions as funct
    df.withColumn('new_column', funct.lower(funct.col('column_name')))