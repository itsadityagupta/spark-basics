import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object HouseDataPipeline extends App {

  case class House(id: Int, area: Int, numBedrooms: Int, numBathrooms: Int, location: String, price: Int, rent: Int);

  private val spark =
    SparkSession
      .builder()
      .master("local")
      //.config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .appName("House Data Pipeline")
      .getOrCreate();

  private val houseSchema: StructType = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("area", IntegerType, nullable = false),
    StructField("numBedrooms", IntegerType, nullable = false),
    StructField("numBathrooms", IntegerType, nullable = false),
    StructField("location", StringType, nullable = false),
    StructField("price", IntegerType, nullable = false),
    StructField("rent", IntegerType, nullable = false)
  ));

  private val houseDataDF = spark.read.option("header", "true").schema(houseSchema)
    .csv("src/main/resources/house_data.csv");

  import spark.implicits._
  val houseDataDS = houseDataDF.as[House]

  def getMostExpensiveHousePerLocationDF(data: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("location").orderBy(col("price").desc);
    val mostExpensiveHouses: DataFrame = data.withColumn("rank", row_number().over(windowSpec));
    mostExpensiveHouses.filter(expr("rank = 1")).drop("rank");
  }

  val mostExpensiveHomesDF = getMostExpensiveHousePerLocationDF(houseDataDF)
  mostExpensiveHomesDF.show()

  mostExpensiveHomesDF.selectExpr("*").orderBy(col("location")).show()

  def getMostExpensiveHousePerLocationDS(data: Dataset[House]): Dataset[House] = {
    val windowSpec = Window.partitionBy("location").orderBy(col("price").desc);
    val expensiveHouse: DataFrame = data.withColumn("rank", row_number().over(windowSpec));
    expensiveHouse
      .filter(expr("rank = 1"))
      .drop("rank")
      .as[House]
  }

  private val mostExpensiveHomesDS: Dataset[House] = getMostExpensiveHousePerLocationDS(houseDataDS)
  mostExpensiveHomesDS.orderBy(col("location")).show()

  // count number for different locations in the data using RDD

  private def countLocationsRR(data: DataFrame): Long = {
    val dataRDD = data.rdd;
    val dataRDDHouse: RDD[House] = dataRDD.map(row => House(
      row.getAs[Int]("id"),
      row.getAs[Int]("area"),
      row.getAs[Int]("numBedrooms"),
      row.getAs[Int]("numBathrooms"),
      row.getAs[String]("location"),
      row.getAs[Int]("price"),
      row.getAs[Int]("rent")
    ));

    val location = dataRDDHouse.map(house => house.location).distinct;
    location.count();
  }

  // TODO: Implement mostExpensive wale functions using RDD

  val numLocations = countLocationsRR(houseDataDF)
  numLocations
}
