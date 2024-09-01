import java.io.{BufferedWriter, FileWriter}
import scala.util.Random

object HouseDataGenerator {

  def main(args: Array[String]): Unit = {
    val numberOfRows = 10000 // Change this value to generate more or fewer rows
    val outputFilePath = "house_data.csv"

    val fileWriter = new BufferedWriter(new FileWriter(outputFilePath))
    fileWriter.write("HouseID,SizeSqFt,Bedrooms,Bathrooms,Location,PriceUSD,RentUSD\n")

    for (i <- 1 to numberOfRows) {
      val houseID = i
      val sizeSqFt = Random.between(500, 5000) // House size between 500 and 5000 sq ft
      val bedrooms = Random.between(1, 6) // Number of bedrooms between 1 and 5
      val bathrooms = Random.between(1, 4) // Number of bathrooms between 1 and 3
      val location = generateRandomLocation()
      val priceUSD = sizeSqFt * Random.between(100, 500) // Price based on size
      val rentUSD = (priceUSD * Random.between(0.002, 0.01)).toInt // Rent as a percentage of price

      fileWriter.write(s"$houseID,$sizeSqFt,$bedrooms,$bathrooms,$location,$priceUSD,$rentUSD\n")
    }

    fileWriter.close()
    println(s"Generated $numberOfRows rows of data and saved to $outputFilePath")
  }

  private def generateRandomLocation(): String = {
    val locations = Seq("New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
      "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose")
    locations(Random.nextInt(locations.length))
  }
}
