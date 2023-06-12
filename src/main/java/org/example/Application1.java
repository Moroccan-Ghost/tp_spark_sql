package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Application1 {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder().appName("TTP SPARK SQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csv =  sparkSession.read().option("header","true").csv("services.csv");

        //Q1
        System.out.println("Incidents par service");
        csv.groupBy(col("service")).count().show();

        //Q2
        System.out.println("Les deux annees ou il y avait plus d'incidents");
        Dataset<Row> incidentPerYear = csv.withColumn("year",col("date")).groupBy("year").count();
        incidentPerYear.orderBy(col("count").desc()).limit(2).show();



    }
}
