import org.apache.spark.sql.SparkSession
import java.util.Scanner

object P1 {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\Users\\Shahrez\\Documents\\Revature\\winutils")

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("created spark session")
    var keyboard = new Scanner(System.in);

    // Load data and create master table
    spark.sql("DROP VIEW IF EXISTS master");
    spark.sql("DROP TABLE IF EXISTS Branches");
    spark.sql("DROP TABLE IF EXISTS consumerCounts");
    spark.sql("DROP TABLE IF EXISTS partitionedTable");
    spark.sql("DROP TABLE IF EXISTS modifiedTable");
    spark.sql("CREATE TABLE IF NOT EXISTS Branches(Beverage String, Branch String)" +
      " row format delimited fields terminated by ','");
    spark.sql("CREATE TABLE IF NOT EXISTS consumerCounts(Beverage String, Count Int)" +
      " row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchA.txt' INTO TABLE Branches");
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchB.txt' INTO TABLE Branches");
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchC.txt' INTO TABLE Branches");
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_ConscountA.txt' INTO TABLE consumerCounts");
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_ConscountB.txt' INTO TABLE consumerCounts");
    spark.sql("LOAD DATA LOCAL INPATH 'Bev_ConscountC.txt' INTO TABLE consumerCounts");
    spark.sql("CREATE VIEW master AS " +
      "SELECT * FROM " +
      "Branches JOIN consumerCounts USING (Beverage) " +
      "ORDER BY Beverage, Branch, Count");
    print("Which problem scenario would you like to run? ")
    val input = keyboard.nextLine();
    if (input.equals("1a")) {
      println("Running scenario 1a...")
      // Problem Scenario 1a:
      val x = spark.sql("SELECT Branch, SUM(Count) AS Total_Number_of_Consumers\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch1'\n" +
        "GROUP BY Branch\n" +
        "ORDER BY Branch");
      x.show(x.count().toInt);
    } else if (input.equals("1b")) {
      // Problem Scenario 1b:
      println("Running scenario 1b...");
      val x = spark.sql("SELECT Branch, SUM(Count) AS Total_Number_of_Consumers\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch2'\n" +
        "GROUP BY Branch\n" +
        "ORDER BY Branch");
      x.show(x.count().toInt);
    } else if (input.equals("2a")) {
      // Problem Scenario 2a:
      println("Running scenario 2a...")
      val x = spark.sql("WITH temp AS (\n" +
        "SELECT Beverage, SUM(Count) as Total_Count\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch1'\n" +
        "GROUP BY Beverage\n" +
        "ORDER BY Total_Count DESC, Beverage ASC\n" +
        "),\n" +
        "temp2 AS (SELECT Beverage, Total_Count, " +
        "DENSE_RANK() OVER (ORDER BY Total_Count DESC) AS Rank\n" +
        "FROM temp)\n" +
        "SELECT Beverage\n" +
        "FROM temp2\n" +
        "WHERE Rank = 1");
      x.show(x.count().toInt);
    } else if (input.equals("2b")) {
      // Problem Scenario 2b
      println("Running scenario 2b...");
      val x = spark.sql("WITH temp AS (\n" +
        "SELECT Beverage, SUM(Count) as Total_Count\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch2'\n" +
        "GROUP BY Beverage\n" +
        "ORDER BY Total_Count ASC, Beverage ASC\n" +
        "),\n" +
        "temp2 AS (SELECT Beverage, Total_Count, " +
        "DENSE_RANK() OVER (ORDER BY Total_Count ASC) AS Rank\n" +
        "FROM temp)\n" +
        "SELECT Beverage\n" +
        "FROM temp2\n" +
        "WHERE Rank = 1");
      x.show(x.count().toInt);
    } else if (input.equals("2c")) {
      // Problem scenario 2c
      println("Running scenario 2c...");
      val x = spark.sql("WITH temp AS (\n" +
        "SELECT Beverage, SUM(Count) as Total_Count\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch2'\n" +
        "GROUP BY Beverage\n" +
        "ORDER BY Total_Count DESC, Beverage ASC\n" +
        "),\n" +
        "temp2 AS (SELECT Beverage, Total_Count, " +
        "DENSE_RANK() OVER (ORDER BY Total_Count DESC) AS Rank\n" +
        "FROM temp)\n" +
        "SELECT Beverage\n" +
        "FROM temp2\n" +
        "WHERE Rank = 1");
      x.show(x.count().toInt);
    } else if (input.equals("3a")) {
      // Problem Scenario 3a
      println("Running scenario 3a...")
      val x = spark.sql("SELECT Beverage\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch8' OR Branch = 'Branch10'\n" +
        "GROUP BY Beverage\n" +
        "ORDER BY Beverage ASC");
      x.show(x.count().toInt);
    } else if (input.equals("3b")) {
      // Problem scenario 3b
      println("Running scenario 3b...")
      val x = spark.sql("WITH temp1 AS (\n" +
        "SELECT Beverage\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch4'\n" +
        "),\n" +
        "temp2 AS (\n" +
        "SELECT Beverage\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch7'\n" +
        ")\n" +
        "SELECT * FROM temp1 JOIN temp2 USING (Beverage)\n" +
        "GROUP BY Beverage\n" +
        "ORDER BY Beverage ASC");
      x.show(x.count().toInt);
    } else if (input.equals("4")) {
      // Problem scenario 4
      println("Running scenario 4...");
      val x = spark.sql("WITH temp1 AS (\n" +
        "SELECT Beverage, Branch\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch4'\n" +
        "GROUP BY Beverage, Branch\n" +
        "),\n" +
        "temp2 AS (\n" +
        "SELECT Beverage, Branch\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch7'\n" +
        "GROUP BY Beverage, Branch\n" +
        ")\n" +
        "SELECT Beverage, temp1.Branch AS BranchA, temp2.Branch AS BranchB FROM temp1 JOIN temp2 USING (Beverage)\n" +
        "GROUP BY Beverage, BranchA, BranchB\n" +
        "ORDER BY Beverage ASC");
      x.write.partitionBy("BranchA").bucketBy(5, "Beverage").saveAsTable("partitionedTable");
      x.createOrReplaceTempView("temporaryview");
      println("Partitioned and Bucketed table:")
      spark.sql("SELECT * FROM partitionedTable").show();
      println("View:")
      spark.sql("SELECT * FROM temporaryview").show();
    } else if (input.equals("6")) {
      println("Running scenario 6...");
      println("Deleting last row from problem scenario 3b...");
      val x = spark.sql("WITH temp1 AS (\n" +
        "SELECT Beverage\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch4'\n" +
        "),\n" +
        "temp2 AS (\n" +
        "SELECT Beverage\n" +
        "FROM master\n" +
        "WHERE Branch = 'Branch7'\n" +
        ")\n" +
        "SELECT * FROM temp1 JOIN temp2 USING (Beverage)\n" +
        "GROUP BY Beverage\n" +
        "ORDER BY Beverage ASC");
      x.show(x.count().toInt - 1); // spark does not support update or delete


    } else {
      println("Invalid input");
      print("Shutting down program");
    }
    spark.sql("DROP VIEW master");
    spark.sql("DROP TABLE Branches");
    spark.sql("DROP TABLE consumerCounts");







  }

}
