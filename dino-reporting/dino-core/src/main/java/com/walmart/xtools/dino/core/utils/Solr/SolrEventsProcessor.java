package com.walmart.xtools.dino.core.utils.Solr;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.IOException;
import java.util.*;

public class SolrEventsProcessor {

   /* private SolrEventsProcessor() {
    }

    public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException, InterruptedException {
        if (args.length < 4) {
            System.err
                    .println("Usage: SolrEventsProcessor <brokers> <topics> <zk_url> <index_name>\n"
                            + "  <brokers> is a list of one or more Kafka brokers\n"
                            + "  <topics> is a list of one or more kafka topics to consume from\n"
                            + " <zk_url> zookeeper url\n"
                            + " <index_name> name of solr index\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];
        String zk_url = args[2];
        String index_name = args[3];

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new DefaultScalaModule());

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf()
                .setAppName("spark-solr");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
        jssc.sparkContext().setLogLevel("ERROR");

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "otto-solr-spark");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);
        kafkaParams.put("bootstrap.servers", brokers);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        JavaDStream<String> events = messages.map(ConsumerRecord::value);

        //convert to SolrDocuments
        JavaDStream<SolrInputDocument> parsedSolrEvents = events.map(incomingRecord -> EventParseUtil.convertData(incomingRecord));

        //send to solr
        SolrSupport.indexDStreamOfDocs(zk_url, index_name, 10, parsedSolrEvents.dstream());

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }*/
}
