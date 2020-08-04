'''
    This python file consists of basic pyspark functions with examples
    Functions in the files
    1) Lower case data                                      2) Case statement in Python
    3) Converting to another data type                      4) Distinct records using Spark
'''


# convert the data to the lower case
    from pyspark.sql import functions as funct
    df.withColumn('new_column', funct.lower(funct.col('column_name')))
    
    
# Use of when in Pyspark
    from pyspark.sql.functions import when
    df2.withColumn('is_abnormal', when(df2.is_abnormal == 'n', 1).when(df2.is_abnormal == 'a', 2).when(df2.is_abnormal == 'l', 3).when(df2.is_abnormal == 'p', 4))

    
# Cast string column to int
    df3.withColumn('is_abnormal', df3.is_abnormal.cast('Int'))

    
# Select Distinct Records
    df3.select('column_name').distinct()