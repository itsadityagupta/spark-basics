import java.io.{BufferedWriter, FileWriter}
import scala.util.Random

object TransactionsDataGenerator extends App {

  private val numAccounts = 250
  private val numTransactions = 50000
  private val invalidTransactionPercentage = 0.5 // 10% of transactions will be invalid

  // Generate account data
  private val accounts = (1 to numAccounts).map { i =>
    val accountNumber = 1000000 + i
    val balance = Random.between(1000, 1000000)
    (accountNumber, balance)
  }

  // Write account data to accounts.csv
  private val accountFile = new BufferedWriter(new FileWriter("accounts.csv"))
  accountFile.write("accountNumber,balance\n")
  accounts.foreach { case (accountNumber, balance) =>
    accountFile.write(s"$accountNumber,$balance\n")
  }
  accountFile.close()

  // Generate transaction data
  private val transactions = (1 to numTransactions).map { i =>
    val fromAccount = accounts(Random.nextInt(accounts.size))._1
    val toAccount = if (Random.nextDouble() < invalidTransactionPercentage) {
      // Generate an invalid account number (not in the accounts list)
      2000000 + i
    } else {
      // Select a valid account number
      accounts(Random.nextInt(accounts.size))._1
    }
    val transferAmount = Random.between(100, 10000)
    (fromAccount, toAccount, transferAmount)
  }

  // Write transaction data to transactions.csv
  private val transactionFile = new BufferedWriter(new FileWriter("transactions.csv"))
  transactionFile.write("fromAccountNumber,toAccountNumber,transferAmount\n")
  transactions.foreach { case (fromAccount, toAccount, transferAmount) =>
    transactionFile.write(s"$fromAccount,$toAccount,$transferAmount\n")
  }
  transactionFile.close()

  println("Data generation with invalid transactions completed!")
}
