package com.apache.spark.PoliceCalls;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.mongodb.spark.MongoSpark;

public class App {
    public static void main( String[] args ){
    	System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");   
    	
    	StructType schema = new StructType().add("recordId",DataTypes.IntegerType)
    		.add("callDateTime",DataTypes.StringType)
    		.add("priority",DataTypes.StringType)
    		.add("district",DataTypes.StringType)
    		.add("description",DataTypes.StringType)
    		.add("callNumber",DataTypes.StringType)
    		.add("incidentLocation",DataTypes.StringType)
    		.add("location",DataTypes.StringType);
    	
    	SparkSession session = SparkSession.builder()
    			.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/police.callcenter")
    			.master("local")
    			.appName("Police Call Center Service")
    			.getOrCreate();    	
    	Dataset<Row> rawData = session.read().option("header", true).schema(schema).csv("dataset/police911.csv");
    	
    	Dataset<Row> dataNotNullDS = rawData.filter(rawData.col("recordId").isNotNull());		// Count: 3053219    	not null data    	    	
    	Dataset<Row> descDS = dataNotNullDS.filter(dataNotNullDS.col("description").notEqual("911/NO  VOICE"));		//  911/NO  VOICE not in description
    	
    	Dataset<Row> resultDS = descDS.groupBy("incidentLocation","description").count().sort(functions.desc("count"));  // Count : 792203
    	
    	// Write the data to the "callcenter" collection
        MongoSpark.write(resultDS).mode("overwrite").save();   // overwrite : old data is deleted, append : adds to the end
    	
    	
   
    }
}
