package demo;

import java.io.IOException;

import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import models.DataEvent;

public class Demo {
	public static void main(String[] args) throws TimeoutException, StreamingQueryException, ParseException, IOException {
		try (SparkSession spark = SparkSession.builder().appName("Read kafka").getOrCreate()) {
			Dataset<String> data = spark
					.readStream()
			        .format("kafka")
			        .option("kafka.bootstrap.servers", "localhost:9092")
			        .option("subscribe", "tiki-2")
			        .load()
					.selectExpr("CAST(value AS STRING)")
					.as(Encoders.STRING()); // load data from kafka
			
			Encoder<DataEvent> successEventEncoder = Encoders.bean(DataEvent.class);
			Dataset<DataEvent> dataSuccess = data.map(new MapFunction<String, DataEvent>() {
				private static final long serialVersionUID = 1L;
				@Override
				public DataEvent call(String value) throws Exception {
					JSONParser parse = new JSONParser();
					JSONObject json = (JSONObject) parse.parse(value);
					
					long guid = (long) json.get("guid");
					long time = (long) json.get("time");
					String url = (String) json.get("url");
					
					DataEvent success = new DataEvent(time, url, guid);
					return success;
				}
			}, successEventEncoder); // convert Dataset<String> to Dataset<SuccessEvent>
			
			Dataset<Row> dataSuccessRow = dataSuccess.toDF(); // convert Dataset<SuccessEvent> to Dataset<Row> (Dataframe)
			
			Dataset<Row> dataView = spark
					.readStream()
					.schema(new StructType() // if get data streaming, must create struct type of data
							.add("time_create", "long")
							.add("cookie_create", "long")
							.add("browser_code", "integer")
							.add("browser_ver", "string")
							.add("os_code", "integer")
							.add("os_ver", "string")
							.add("ip", "long")
							.add("guid", "long")
							.add("domain", "string")
							.add("path", "string")
							.add("geo", "integer")
							.add("locid", "integer")
							.add("flashver", "string")
							.add("jre", "string")
							.add("siteid", "integer")
							.add("channelid", "integer")
							.add("refer", "string")
							.add("sr", "string")
							.add("sc", "string")
							.add("fullrefer", "string")
							.add("requestid", "long")
							.add("fp_guid", "string")
							.add("category", "string")
							.add("googleId", "string")
							.add("tabActive", "string"))
					.parquet("/home/trannguyenhan/dataset/data/demo_view"); 
			dataView =  dataView.filter(dataView.col("domain").contains("tiki"));
			
			dataView.createOrReplaceTempView("dataView");
			dataSuccessRow.createOrReplaceTempView("dataSuccess");
			spark.sql("select dataSuccess.guid, dataSuccess.url, dataView.time_create, dataSuccess.timeCreate"
					+ " from dataView inner join dataSuccess on dataView.guid = dataSuccess.guid").createOrReplaceTempView("dataJoin");
			
			Dataset<Row> dataResult = spark.sql("select * from dataJoin where time_create between timeCreate and timeCreate + 3600000");
			
			dataResult.printSchema();
			
			// print to console
//			StreamingQuery query = dataResult.writeStream()
//					.trigger(Trigger.ProcessingTime(10000))
//					.outputMode("append")
//					.format("console")
//					.start();
			
			// print to parquet file
			StreamingQuery query = dataResult.writeStream()
		    .format("parquet")        // can be "orc", "json", "csv", etc.
		    .option("checkpointLocation", "/home/trannguyenhan/dataset/data/result")
		    .option("path", "/home/trannguyenhan/123")
		    .start();
			
			query.awaitTermination();
			
			
		}
	}
	
	
}
