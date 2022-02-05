import org.apache.flink.api.scala._


case class Title (Make: String, Model: String, Year: Int, Engine_Fuel_Type: String,
                  Engine_HP: Int, Engine_Cylinders: Int, Transmission_Type: String,
                  Driven_Wheels: String, Number_of_Doors: Int, Market_Category: String,
                  Vehicle_Size: String, Vehicle_Style: String, highway_MPG: Int,
                  city_mpg: Int, Popularity: Int, MSRP: Int)

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello, it's my homework!")

    val env = ExecutionEnvironment.getExecutionEnvironment

    val csvInput: DataSet[Title] = env.readCsvFile[Title]("data.csv", "\n", ",", ignoreFirstLine = true)

    csvInput.print()

    val max_engineHP = csvInput
      .map(x => (x.Make, x.Year, x.Engine_HP))
      .groupBy(0, 1)
      .max(2)

    val min_engineHP = csvInput
      .map(x => (x.Make, x.Year, x.Engine_HP))
      .groupBy(0, 1)
      .min(2)

    max_engineHP.print()
    min_engineHP.print()


    env.execute("CSV batch-processing")
  }
}
