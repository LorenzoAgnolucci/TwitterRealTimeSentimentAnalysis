import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
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

        Job job = Job.getInstance(conf, "BatchTwitterSentimentAnalysis");

        job.addCacheFile(new URI(CLASSIFIER_MODEL));

        job.setJarByClass(HadoopDriver.class);

        TableMapReduceUtil.initTableMapperJob("tweet_master_database", scan, HadoopMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("tweet_batch_view", HadoopReducer.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception{
        while(true){
            ToolRunner.run(new HadoopDriver(), args);
            Thread.sleep(30 * 1000);
        }
    }
}
