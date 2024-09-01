import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.concurrent.duration.DurationInt

object TransactionsPipeline extends App {

  private def extractValidTransactions(accounts: DataFrame, transactions: DataFrame): DataFrame = {

    transactions
     .alias("a")
     .join(
       // functions.broadcast(accounts).alias("b"),
       accounts.alias("b"),
       functions.expr("a.to_account = b.account_no")
     )
       .join(
         // functions.broadcast(accounts).alias("c"),
         accounts.alias("c"),
         functions.expr("a.from_account = c.account_no AND a.transfer_amount <= c.balance")
       )
       .selectExpr("a.*")
  }

  private def transactionsPerAccount(transactions: DataFrame): DataFrame = {
    val counts = transactions.groupBy("from_account").agg(functions.countDistinct("to_account", "transfer_amount").as("distinct_transactions"));
    counts.limit(10) //.orderBy(functions.col("distinct_transactions").desc).limit(10);
  }

  private val spark =
    SparkSession
      .builder()
      .master("local")
      //.config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .appName("Banking Data Mining")
      .getOrCreate();

//  private val accountsSchema: StructType = StructType(Array(
//    StructField("accountNumber", LongType, nullable = false),
//    StructField("balance", IntegerType, nullable = false)
//  ));
//
//  private val transactionsSchema: StructType = StructType(Array(
//    StructField("fromAccountNumber", LongType, nullable = false),
//    StructField("toAccountNumber", LongType, nullable = false),
//    StructField("transferAmount", IntegerType, nullable = false)
//  ));

  private val accountsSchema: StructType = StructType(Array(
    StructField("account_no", LongType, nullable = false),
    StructField("balance", IntegerType, nullable = false)
  ));

  private val transactionsSchema: StructType = StructType(Array(
    StructField("from_account", LongType, nullable = false),
    StructField("to_account", LongType, nullable = false),
    StructField("transfer_amount", IntegerType, nullable = false)
  ));

  val accounts: DataFrame =
    spark.read
      .option("header", "true")
      .schema(accountsSchema)
      .csv("src/main/resources/accounts.csv");

  val transactions: DataFrame =
    spark.read
      .option("header", "true")
      .schema(transactionsSchema)
      .csv("src/main/resources/transactions.csv");


  extractValidTransactions(accounts, transactions).show(10);

  transactionsPerAccount(transactions).show(20);

  Thread.sleep(10.minutes.toMillis);

}
