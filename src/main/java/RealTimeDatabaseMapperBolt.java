import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RealTimeDatabaseMapperBolt extends BaseRichBolt{
    private String tableName;
    private Table table;


    public RealTimeDatabaseMapperBolt(String tableName){
        this.tableName = tableName;

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector){
        try{
            Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            this.table = connection.getTable(TableName.valueOf(tableName));
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple){
        String keyword = (String) tuple.getValueByField("keyword");
        String sentiment = (String) tuple.getValueByField("sentiment");
        String rowKey = String.valueOf(System.currentTimeMillis()) + "_" + String.valueOf(ThreadLocalRandom.current().nextLong(0, 100000));
        Put put = new Put(Bytes.toBytes(rowKey))
                .addColumn(Bytes.toBytes("content"), Bytes.toBytes("keyword"), Bytes.toBytes(keyword))
                .addColumn(Bytes.toBytes("content"), Bytes.toBytes("sentiment"), Bytes.toBytes(sentiment));
        try{
            table.put(put);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){}
}
