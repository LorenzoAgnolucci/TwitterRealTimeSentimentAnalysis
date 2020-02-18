import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class MasterDatabaseMapperBolt extends BaseRichBolt{

    private String tableName;
    private Table table;


    public MasterDatabaseMapperBolt(String tableName){
        this.tableName = tableName;

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector){
        try{
            Configuration conf = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(conf);
            this.table = connection.getTable(TableName.valueOf(tableName));
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple){
        String tweetID = (String) tuple.getValueByField("tweet_ID");
        String text = (String) tuple.getValueByField("text");
        ArrayList<String> keywords = (ArrayList<String>) tuple.getValueByField("keywords");
        Put newRow = new Put(Bytes.toBytes(tweetID));
        newRow.addColumn(Bytes.toBytes("content"), Bytes.toBytes("text"), Bytes.toBytes(text));
        newRow.addColumn(Bytes.toBytes("content"), Bytes.toBytes("keywords"), WritableUtils.toByteArray(DatabaseUtils.toWritable(keywords)));
        try{
            table.put(newRow);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){}
}
