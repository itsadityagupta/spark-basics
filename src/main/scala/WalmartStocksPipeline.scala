import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lit, to_date, to_timestamp}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

import java.sql.Date
import scala.concurrent.duration.DurationInt

object WalmartStocksPipeline extends App {

  // data: https://finance.yahoo.com/quote/WMT/history/?guccounter=1&guce_referrer=aHR0cHM6Ly93d3cuZ29vZ2xlLmNvLmluLw&guce_referrer_sig=AQAAAIrzV_xkunL9Pj0i0Gwaxpjk_czgyffbxjEOqrTyYZiV2gXk8W8AjDqhU5TbZuQgfTtFjjLO70MSSqR0PgNhQe3igi6uiwb5pM82ht3_Q5B9JUPqJ3JSW5EBoMMDUZDfhJ7AdA-O2jXknU5zenQsr40PtXVtVCE1VRyRmIk5CmQu&period1=1262304000&period2=1725200046
  // from 1st Jan 2010 to 1st Sept 2024

  val spark = SparkSession
    .builder()
    .appName("Walmart Stock Price Analysis")
    .master("local[*]")
    .getOrCreate();

  private val walmartStocksSchema: StructType = StructType(Array(
    StructField("date", DateType, nullable = false),
    StructField("open", DoubleType, nullable = false),
    StructField("high", DoubleType, nullable = false),
    StructField("low", DoubleType, nullable = false),
    StructField("close", DoubleType, nullable = false),
    StructField("adj_close", DoubleType, nullable = false),
    StructField("volume", LongType, nullable = false)
  ));

  private val walmartStocksDF = spark
    .read
    .option("header", "true")
    .schema(walmartStocksSchema)
    .csv("src/main/resources/walmart_stocks_data.csv");

  walmartStocksDF.show(10);

  walmartStocksDF.printSchema();

  private case class WalmartStocks(date: Date, open: Double, high: Double, low: Double, close: Double, adj_close: Double, volume: Long);

  import spark.implicits._

  private val walmartStocksDS = walmartStocksDF.as[WalmartStocks]

  walmartStocksDS.show(10)

  walmartStocksDF
    .withColumn("year", functions.year(col("date")))
    .withColumn("month", functions.month(col("date")))
    .withColumn("day", functions.day(col("date")))
    .show(10);


  val spec = Window.orderBy("date")
  walmartStocksDF
    .withColumn("prev_adj_close", functions.lag("adj_close", 1, 0).over(spec))
    .withColumn("daily_returns_pct", functions.try_multiply(functions.try_divide(col("adj_close") - col("prev_adj_close"), col("prev_adj_close")), lit(100)))
    .show(10);

  walmartStocksDF
    .filter(functions.expr("volume >= 50000000"))
    .selectExpr("max(volume)")
    .show()

  walmartStocksDF
    .withColumn("year", functions.year(col("date")))
    .groupBy("year")
    .agg(
      functions.avg("high").as("avg_high"),
      functions.avg("low").as("avg_low"),
      functions.avg("open").as("avg_open"),
      functions.avg("close").as("avg_close")
    )
    .selectExpr("*")
    .show(50);

  private val highestClosePrice = walmartStocksDF.agg(functions.max("close")).first().get(0)
  print(highestClosePrice)

  walmartStocksDF.filter(functions.expr(s"close = $highestClosePrice")).show(10)

  private val highestClosePrice2 = walmartStocksDF.orderBy(col("close").desc).first();

  walmartStocksDF
    .withColumn("volatility_pct", functions.expr("((high - low)*100)/high"))
    .filter(functions.expr("ABS(volatility_pct) >= 5.0"))
    .selectExpr("*")
    .show(20);

  private val anotherSpec = Window.orderBy("date_seconds").rangeBetween(-30 * 86400L, 0);

  walmartStocksDF
    .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd"))
    .withColumn("date_seconds", functions.unix_seconds(col("date")))
    .withColumn("moving_close_avg", functions.avg("close").over(anotherSpec))
    .show(20);

  private val yetAnotherSpec = Window.orderBy("year")

  walmartStocksDF
    .withColumn("year", functions.year(col("date")))
    .groupBy("year")
    .agg(avg("adj_close").as("avg_adj_close"))
    .withColumn("prev_avg_adj_close", functions.lag("avg_adj_close", 1, 0).over(yetAnotherSpec))
    .withColumn("yoy_growth_pct", functions.expr("CASE WHEN prev_avg_adj_close = 0 THEN 100 ELSE ((avg_adj_close - prev_avg_adj_close)*100.0)/prev_avg_adj_close END"))
    .selectExpr("*")
    .show(20)


}
