import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;


public class HadoopMapper extends TableMapper<Text, Text>{

    private SentimentClassifier classifier;
    private long startTimestamp;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{

        Configuration configuration = context.getConfiguration();
        startTimestamp = Long.parseLong(configuration.get("start"));

        File modelFile = new File(context.getCacheFiles()[0].toString());
        try{
            classifier = new SentimentClassifier(modelFile);
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException{
        long check = value.rawCells()[0].getTimestamp();

        if(check <= startTimestamp){
            byte[] byteText = value.getValue(Bytes.toBytes("content"), Bytes.toBytes("text"));
            String text = new String(byteText);
            byte[] byteKeywords = value.getValue(Bytes.toBytes("content"), Bytes.toBytes("keywords"));
            ArrayWritable writable = new ArrayWritable(Text.class);
            writable.readFields(new DataInputStream(new ByteArrayInputStream(byteKeywords)));
            ArrayList<String> keywords = DatabaseUtils.fromWritable(writable);

            String sentiment = classifier.classify(text);
            for(String keyword: keywords){
                context.write(new Text(keyword), new Text(sentiment));
            }
        }

    }
}
