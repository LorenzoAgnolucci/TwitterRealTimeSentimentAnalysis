import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.net.URI;

public class HadoopDriver extends Configured implements Tool{

    public static final String CLASSIFIER_MODEL = "SentimentClassifierTrainedModel.model";

    @Override
    public int run(String[] args) throws Exception{
        System.out.println("Starting MapReduce");

        BasicConfigurator.configure();
        Configuration conf = new Configuration();


        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("content"));

        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        Table table = connection.getTable(TableName.valueOf("synchronization_table"));

        table.put(new Put(Bytes.toBytes("MapReduce_start_timestamp"))
                .addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));

        long start = System.currentTimeMillis();
        conf.setLong("start", start);

        Job job = Job.getInstance(conf, "BatchTwitterSentimentAnalysis");







        job.addCacheFile(new URI(CLASSIFIER_MODEL));

        job.setJarByClass(HadoopDriver.class);



        TableMapReduceUtil.initTableMapperJob("tweet_master_database", scan, HadoopMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("tweet_batch_view", HadoopReducer.class, job);




        System.out.println("\n\nStart: " + start + "\n\n");

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        long end = System.currentTimeMillis();

        System.out.println("\n\nEnd: " + end);
        long elapsed = end - start;
        System.out.println("\n\nElapsed: " + elapsed);


        table.put(new Put(Bytes.toBytes("MapReduce_end_timestamp"))
                .addColumn(Bytes.toBytes("placeholder"), Bytes.toBytes(""), Bytes.toBytes("")));

        return returnValue;
    }


    public static void main(String[] args) throws Exception{
        while(true){
            ToolRunner.run(new HadoopDriver(), args);
            Thread.sleep(30 * 1000);
        }
    }
}
