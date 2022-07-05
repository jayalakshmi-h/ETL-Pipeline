import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object writeParquet extends App{

  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()

  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql:postgres")
    .option("dbtable", "data")
    .option("user", "postgres")
    .option("password", "password")
    .option("numPartitions",100)
    .option("partitionColumn","gph")
    .option("lowerBound", 0)
    .option("upperBound", 60000)
    .load()


  val df = jdbcDF.withColumn("pKey", ceil(col(("gph"))/1000))
  df.cache()

  df.write.partitionBy("pKey").parquet("/Users/jayalakshmi/Downloads/output")

}
