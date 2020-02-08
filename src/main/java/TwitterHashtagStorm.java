import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.com.google.common.io.Files;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

//TODO: Learn how HBase mapper works
//TODO: Find a way to add tweets to tables correctly and efficiently

public class TwitterHashtagStorm {
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


        if(!admin.tableExists(TableName.valueOf("tweet_realtime_view"))){
            TableDescriptor tweetRealtimeView = TableDescriptorBuilder.newBuilder(TableName.valueOf("tweet_realtime_view"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("sentiment_count".getBytes()).build())
                    .build();
            admin.createTable(tweetRealtimeView);
        }

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TweetStreamSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keywords));

        builder.setBolt("twitter-parser-bolt", new TweetParserBolt(keywords))
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-database-mapper-bolt", new TweetDatabaseMapperBolt("tweet_master_database"))
                .shuffleGrouping("twitter-parser-bolt");

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
        Thread.sleep(100000);
        cluster.shutdown();
    }
}