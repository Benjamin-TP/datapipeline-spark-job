# Data Pipeline project - RTE data

This project is part of the evaluation for the *Data Pipeline* class at DSTI.

## Project description - data

The aim is to extract and compute reports on electricity data from RTE for the year 2020.

Sources:
<ul>
<li>Electricity production data for 2020: https://eco2mix.rte-france.com/download/eco2mix/eCO2mix_RTE_Annuel-Definitif_2020.zip</li>
<li>t CO2 eq /MWh per source: https://www.rte-france.com/en/eco2mix/co2-emissions
    <ul>
        <li>0.986 t CO2 eq /MWh for coal-fired plants (Charbon)</li>
        <li>0.777 t CO2 eq /MWh for oil-fired plants (Fioul)</li>
        <li>0.429 t CO2 eq /MWh for gas-fired plants (Gaz)</li>
        <li>0.494 t CO2 eq /MWh for biofuel plants (waste)</li>
    </ul>
</li>
</ul>

## Resources
- Project template: https://github.com/jlcanela/simple-job
- EMR serverless template: https://github.com/jlcanela/emr-serverless-template

## Software installation
I used a Windows environment and followed installation instructions here: https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/
- Software available here: https://spark.apache.org/downloads.html
- *winutils.exe* and *hadoop.dll* added to the software *bin* folder.

I also updated the project template:
- *build.sbt* : spark version updated
- *.config("spark.master", "local")* added to the spark session for resolving an "java.lang.UnsatisfiedLinkError" ([Source](https://stackoverflow.com/questions/38008330/spark-error-a-master-url-must-be-set-in-your-configuration-when-submitting-a))

## How to clone the repository on the local dev. environment
```
git clone https://github.com/Benjamin-TP/datapipeline-spark-job.git
```

## How to compile and run the project

### Development phase: with sbt
```
sbt compile
sbt run
```

I also use the following command (the tilde makes the app running continuously, and taking into account the code changes)
```
sbt ~run
```

To create the jar file used for the spark job:
```
sbt assembly
```

### How to run the spark job
spark-submit target/scala-2.12/sample-job-assembly-1.0.jar data/eCO2mix_RTE_Annuel-Definitif_2020.csv 

NB: At the end of the development, I wasn't able anymore to make it work (it is working with the project template). Hence, I used *sbt* (with *sbt run*) to run the program.

## Data extracted / reported
- [CO2_emission.csv](./data/output/CO2_emission.csv): CO2 emissions for the coal / oil / gas using the provided data
- [CO2_emission_report1.csv](./data/output/CO2_emission_report1.csv): report for each 15mn period if the CO2_emission_coal > CO2_emission_oil
- [CO2_emission_report2.csv](./data/output/CO2_emission_report2.csv): report for each 15mn period if the CO2_emission_coal > CO2_emission_oil

NB: I didn't manage to create the CSV files with the right names.
Indeed, the process create *.csv* folders inside the the *data* folder; each folder contains metadata and a CSV file expected

I then manually copied the CSV files created and added them to the *data/output* folder.

folders 
### O. Output files

### 1. Data extraction
The data is extracted from the *eCO2mix_RTE_Annuel-Definitif_2020.csv* is stored in a DataFrame (a spark DataSet) and there is a little bit of reshaping:
- The *Date* field is reformatted (to yyyy-MM-dd format), in order to be used for sorting data (actually it is useful for the second report only but I do it here in order to have the same date format for the three extractions)
- The null / empty fields are fed with 0 (in order to avoid any issue with the calculation)
- The columns to be used are renamed (from french to english)

### 2. CO2_emission.csv 
Columns: “Date,Time,Oil_CO2,Coal_CO2,Gas_CO2” for each data point

A new dataset is created from the one obtained at the previous step.
- Useful columns selected
- *CO2* columns added


### 3. CO2_emission_report1.csv 
Columns: "Date,Time,Coal_CO2_greater_than_Oil_CO2" for each data point

A new dataset is created from the one obtained at the previous step (*CO2_emission*).
- Columns selected from the previous dataframe used
- Boolean added by comparing *Oil_CO2* to *Coal_CO2*
- *_CO2* columns removed

### 3. CO2_emission_report2.csv
Columns: "Date,Coal_CO2_greater_than_Oil_CO2"

From the *CO2_emission* dataframe,
- data grouped by date
- Boolean added by comparing *Oil_CO2* to *Coal_CO2*
- *_CO2* columns removed
- data sorted by date.

## Spark job on AWS
TODO

## Project status / conclusion
The projet is not finished. Hereafter the improvement points/tasks to be achieved.
- XLS file to be loaded (instead of a CSV file). Indeed, I didn't manage to load the XLS file, hence I saved it as a CSV file.
- CSV output files to be saved directly with the desired names
- Spark job running withRun the spark job with *spark-submit*
- Job deployment on a serverless architecture.

As I spent time configuring my Windows environment, I may retry with Linux (Ubuntu), maybe it is easier to configure Spark development environment there.
