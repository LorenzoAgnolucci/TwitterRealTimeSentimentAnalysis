import java.nio.charset.Charset;
import java.util.*;
import java.io.File;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.storm.shade.com.google.common.io.Files;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.conf.Configuration;

//TODO: Do not push the credentials to Github
//TODO: For example read credentials from a txt file

//TODO: Install and configure HBase (possibly also Hadoop)
//TODO: Learn how HBase mapper works
//TODO: Insert dummy data to table and try to read it

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
        String[] keyWords = Arrays.copyOfRange(arguments, 0, arguments.length);

        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TweetStreamSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret, keyWords));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
        Thread.sleep(50000);
        cluster.shutdown();
    }
}