import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

public class TweetRealTimeViewMapperBolt extends BaseRichBolt{

    private String tableName;
    private Table table;

    public TweetRealTimeViewMapperBolt(String tableName){
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
        String keyword = (String) tuple.getValueByField("keyword");
        String sentiment = (String) tuple.getValueByField("sentiment");
        String column;
        if(sentiment.equals("1")){
            column = "positive";
        }
        else{
            column = "negative";
        }
        Get get = new Get(Bytes.toBytes(keyword))
                .addColumn(Bytes.toBytes("sentiment_count"), Bytes.toBytes(column));
        try{
            Result result = table.get(get);
            int newCount;
            if(result.isEmpty()){
                newCount = 1;
            }
            else{

                newCount = Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("sentiment_count"), Bytes.toBytes(column)))) + 1;
            }
            Put newRow = new Put(Bytes.toBytes(keyword))
                    .addColumn(Bytes.toBytes("sentiment_count"), Bytes.toBytes(column), Bytes.toBytes(Integer.toString(newCount)));
            table.put(newRow);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){}
}
