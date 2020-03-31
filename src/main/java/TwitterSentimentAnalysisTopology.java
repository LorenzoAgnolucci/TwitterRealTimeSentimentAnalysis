import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.com.google.common.io.Files;
import org.apache.storm.topology.TopologyBuilder;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;


public class TwitterSentimentAnalysisTopology{
    public static void main(String[] args) throws Exception{

        // Read Twitter API credentials from txt file
        List<String> lines = Files.readLines(new File("TwitterCredentials"), Charset.defaultCharset());
        String consumerKey = Arrays.asList(lines.get(0).split(":")).get(1).trim();
        String consumerSecret = Arrays.asList(lines.get(1).split(":")).get(1).trim();
        String accessToken = Arrays.asList(lines.get(2).split(":")).get(1).trim();
        String accessTokenSecret = Arrays.asList(lines.get(3).split(":")).get(1).trim();

        //Read keywords as an argument
        String[] arguments = args.clone();
        String[] keywords = Arrays.copyOfRange(arguments, 0, arguments.length);

        Config config = new Config();
        config.setDebug(true);

        //HBase tables creation
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if(!admin.tableExists(TableName.valueOf("tweet_master_database"))){
            TableDescriptor tweetMasterDatabase = TableDescriptorBuilder.newBuilder(TableName.valueOf("tweet_master_database"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("content".getBytes()).build())
                    .build();
            admin.createTable(tweetMasterDatabase);
        }


        if(!admin.tableExists(TableName.valueOf("tweet_batch_view"))){
            TableDescriptor tweetBatchView = TableDescriptorBuilder.newBuilder(TableName.valueOf("tweet_batch_view"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("sentiment_count".getBytes()).build())
                    .build();
            admin.createTable(tweetBatchView);
        }


        if(!admin.tableExists(TableName.valueOf("synchronization_table"))){
            TableDescriptor tweetRealtimeView = TableDescriptorBuilder.newBuilder(TableName.valueOf("synchronization_table"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("placeholder".getBytes()).build())
                    .build();
            admin.createTable(tweetRealtimeView);
            Table table = connection.getTable(TableName.valueOf("synchronization_table"));
            table.put(new Put(Bytes.toBytes("MapReduce_start_timestamp"), 0)
                    .addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));
            table.put(new Put(Bytes.toBytes("MapReduce_end_timestamp"), 0)
                    .addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));
        }


        if(!admin.tableExists(TableName.valueOf("tweet_realtime_database"))){
            TableDescriptor tweetRealtimeView = TableDescriptorBuilder.newBuilder(TableName.valueOf("tweet_realtime_database"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("content".getBytes()).build())
                    .build();
            admin.createTable(tweetRealtimeView);
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-stream-spout", new TweetStreamSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keywords));

        builder.setBolt("twitter-parser-bolt", new TweetParserBolt(keywords))
                .shuffleGrouping("twitter-stream-spout");
        
        builder.setSpout("twitter-csv-spout", new TweetCSVSpout());

        builder.setBolt("master-database-mapper-bolt", new MasterDatabaseMapperBolt("tweet_master_database"))
                .shuffleGrouping("twitter-parser-bolt").shuffleGrouping("twitter-csv-spout");

        builder.setBolt("twitter-sentiment-classifier-bolt", new SentimentClassifierBolt("SentimentClassifierTrainedModel.model"))
                .shuffleGrouping("twitter-parser-bolt").shuffleGrouping("twitter-csv-spout");

        builder.setBolt("realtime-database-mapper-bolt", new RealTimeDatabaseMapperBolt("tweet_realtime_database"))
                .shuffleGrouping("twitter-sentiment-classifier-bolt");


        builder.setSpout("synchronization-spout", new SynchronizationSpout("synchronization_table"));

        builder.setBolt("synchronization-bolt", new SynchronizationBolt("tweet_realtime_database"))
                .shuffleGrouping("synchronization-spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterSentimentAnalysisStorm", config,
                builder.createTopology());
        Thread.sleep(1200000);
        cluster.shutdown();
    }
}