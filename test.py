from pyspark.sql import SparkSession

def main():
  print ("What the heck")
  print( "Hello World!" )
  peopleDF = spark.read.json("examples/src/main/resources/people.json")

  # DataFrames can be saved as Parquet files, maintaining the schema information.
  peopleDF.write.parquet("people.parquet")

  # Read in the Parquet file created above.
  # Parquet files are self-describing so the schema is preserved.
  # The result of loading a parquet file is also a DataFrame.
  parquetFile = spark.read.parquet("people.parquet")

  # Parquet files can also be used to create a temporary view and then used in SQL statements.
  parquetFile.createOrReplaceTempView("parquetFile")
  teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
  teenagers.show()
  # +------+
  # |  name|
  # +------+
  # |Justin|
  # +------+
  
if __name__== "__main__":
  main()
