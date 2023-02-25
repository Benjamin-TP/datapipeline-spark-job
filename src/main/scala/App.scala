import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession
// The two below imports are used for the dataframe extracted from the excel file
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions._

object App {
     
    val log = Logger.getLogger(App.getClass().getName())

    def createSession = {
         val builder = SparkSession.builder.appName("Spark Batch")
        //.config("spark.eventLog.enabled", true)
        //.config("spark.eventLog.dir", "./spark-logs")
        .config("spark.master", "local") //Added
        builder.getOrCreate()
    }
    
    // def run(in: String, out: String) = {
    //     log.info("Starting App")
    //     val spark = createSession
    //     val df = spark.read.option("header", true).csv(in)
    //     df.write.mode("Overwrite").json(out)
    //     createSession.close
    //     log.info("Stopping App")
    // }

    def run(input_file: String) = {
        log.info("Starting App")
        val spark = createSession
        
        // Get RTE data from the input file:
        val df_RTE = get_dataset_from_file(spark,input_file)

        // Reshape RTE data
        // - Date formatting (useful for the report 2 - group by date); done here in order to have one unique date format for the three files
        // - Null values fed with 0
        // - Columns renamed
        val df_RTE_reshaped = reshape_RTE_data(df_RTE)

        // Prepare CO2_emission.csv: get Date, Time, Oil_CO2, Coal_CO2, Gas_CO2
        // 1. Set the output path
        // 2. Prepare the Dataset
        // 3. Save as a CSV file
        val outputFileEmission="./data/CO2_emission.csv"
        val df_CO2_emission = prepare_CO2_emission(df_RTE_reshaped)
        // Save data in a CSV file
        save_df_as_csv(df_CO2_emission,outputFileEmission)

        // CO2_emission_report1.csv: 
        // "Date,Time,Coal_CO2_greater_than_Oil_CO2" for each data point (each quarter)
        // Same steps as above
        val outputFileEmissionR1="./data/CO2_emission_report1.csv"
        val df_emission_r1 = prepare_CO2_report1(df_CO2_emission)
        save_df_as_csv(df_emission_r1,outputFileEmissionR1)

        // CO2_emission_report2.csv: 
        // "Date,Coal_CO2_greater_than_Oil_CO2" for each day
        val outputFileEmissionR2="./data/CO2_emission_report2.csv"
        val df_emission_r2 = prepare_CO2_report2(df_CO2_emission)
        save_df_as_csv(df_emission_r2,outputFileEmissionR2)

        // Close session
        createSession.close
        log.info("Stopping App")
    }

    //added
    def run_without_params() = {
        log.info("Starting App")
        val spark = createSession
        
        // Get RTE data from the input file:
        val df_RTE = get_dataset_from_file(spark,"data/eCO2mix_RTE_Annuel-Definitif_2020.csv")

        // Reshape RTE data
        // - Date formatting (useful for the report 2 - group by date); done here in order to have one unique date format for the three files
        // - Null values fed with 0
        // - Columns renamed
        val df_RTE_reshaped = reshape_RTE_data(df_RTE)

        // Prepare CO2_emission.csv: get Date, Time, Oil_CO2, Coal_CO2, Gas_CO2
        // 1. Set the output path
        // 2. Prepare the Dataset
        // 3. Save as a CSV file
        val outputFileEmission="./data/CO2_emission.csv"
        val df_CO2_emission = prepare_CO2_emission(df_RTE_reshaped)
        // Save data in a CSV file
        save_df_as_csv(df_CO2_emission,outputFileEmission)

        // CO2_emission_report1.csv: 
        // "Date,Time,Coal_CO2_greater_than_Oil_CO2" for each data point (each quarter)
        // Same steps as above
        val outputFileEmissionR1="./data/CO2_emission_report1.csv"
        val df_emission_r1 = prepare_CO2_report1(df_CO2_emission)
        save_df_as_csv(df_emission_r1,outputFileEmissionR1)

        // CO2_emission_report2.csv: 
        // "Date,Coal_CO2_greater_than_Oil_CO2" for each day
        val outputFileEmissionR2="./data/CO2_emission_report2.csv"
        val df_emission_r2 = prepare_CO2_report2(df_CO2_emission)
        save_df_as_csv(df_emission_r2,outputFileEmissionR2)

        // Close session
        createSession.close
        log.info("Stopping App")
    }

    def save_df_as_csv(df: Dataset[Row], fileFullPath : String) = {
        df
        //.coalesce(1)
        .repartition(1) // To get one output file only
        .write.mode("Overwrite")
        .format("csv") //com.databricks.spark.csv
        .option("header", "true")
        .option("delimiter", ",")
        .save(fileFullPath)

    }

    def get_dataset_from_file(spark: SparkSession, fileFullPath : String) : Dataset[Row] = {
        val df_file = spark.read
                    .option("header", true)
                    .option("delimiter", ",")
                    .csv("data/eCO2mix_RTE_Annuel-Definitif_2020.csv")
                    .na.fill(0)
        return df_file
    }

    def reshape_RTE_data(df: Dataset[Row]) : Dataset[Row] = {

        val df_reshaped = df.withColumn("Date", date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))
                                    .na.fill("0") // I don't know why fill(0) wasnt working
                                    .withColumnRenamed("Heures", "Time")
                                    .withColumnRenamed("Fioul", "Oil")
                                    .withColumnRenamed("Charbon", "Coal")
                                    .withColumnRenamed("Gaz", "Gas")
        return df_reshaped
    }

    def prepare_CO2_emission(df_input: Dataset[Row]) : Dataset[Row] = {
        // NB: I didn't manage to apply the computation / new column creation in a single select method.
        // Hence, I first create the new columns then drop the useless ones.
        val df_CO2_emission = df_input.select("Date",
                                                "Time",
                                                "Oil",
                                                "Coal",
                                                "Gas"
                                                )
                                            .withColumn("Oil_CO2", df_input("Oil") * 0.777 )
                                            .withColumn("Coal_CO2", df_input("Coal") * 0.986 )
                                            .withColumn("Gas_CO2", df_input("Gas") * 0.429 )
                                            // The MWh values are now useless: we remove them
                                            .drop("Oil")
                                            .drop("Coal")    
                                            .drop("Gas")
        return df_CO2_emission
    }

    def prepare_CO2_report1(df_CO2_emission: Dataset[Row]) : Dataset[Row] = {
        val df_emission_r1 = df_CO2_emission.select(
                                            "Date",
                                            "Time",
                                            "Oil_CO2",
                                            "Coal_CO2")
                                            // Below: boolean added
                                            .withColumn("Coal_CO2_greater_than_Oil_CO2",
                                                        df_CO2_emission("Coal_CO2") > df_CO2_emission("Oil_CO2")
                                                        )
                                            // The MWh values are now useless: we remove them
                                            .drop("Oil_CO2")
                                            .drop("Coal_CO2")
        return df_emission_r1
    }

    def prepare_CO2_report2(df_CO2_emission: Dataset[Row]) : Dataset[Row] = {
        // Below: I had to change the "Date" format in order to sort by date (otherwise, in the CSV file, we get 01/01/2020 then 01/02/2020 etc...)
        val df_emission_r2 = df_CO2_emission
                                        // .withColumn("Date", date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))
                                        .groupBy(col("Date"))
                                        .agg(
                                            sum(col("Oil_CO2")).as("sum_Oil_CO2"),
                                            sum(col("Coal_CO2")).as("sum_Coal_CO2"))
                                        // Below: boolean added
                                        .withColumn("Coal_CO2_greater_than_Oil_CO2",
                                            col("sum_Coal_CO2") > col("sum_Oil_CO2")
                                        )
                                        // The MWh values are now useless: we remove them
                                        .drop("sum_Oil_CO2")
                                        .drop("sum_Coal_CO2")
                                        // Data sorted by date
                                        .sort(col("Date"))
        return df_emission_r2
    }

    def main(args: Array[String]) = args match {
        case Array(in) => run(in)
        case _ => run_without_params()
        // case Array(in, out) => run(in, out)
        // case _ => println("usage: spark-submit app.jar <in.csv> <out.json>")
    }
}