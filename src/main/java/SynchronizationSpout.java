import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.util.Map;

public class SynchronizationSpout extends BaseRichSpout{

    long startTimestamp;
    long endTimestamp;
    String tableName;
    Table table;
    SpoutOutputCollector collector;

    public SynchronizationSpout(String tableName){
        this.startTimestamp = 0;
        this.endTimestamp = 0;
        this.tableName = tableName;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector){
        this.collector = spoutOutputCollector;
        try{
            Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            this.table = connection.getTable(TableName.valueOf(tableName));
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple(){
        try{
            Result startResult = table.get(new Get(Bytes.toBytes("MapReduce_start_timestamp")));
            Result endResult = table.get(new Get(Bytes.toBytes("MapReduce_end_timestamp")));
            long resultStartTimestamp = startResult.rawCells()[0].getTimestamp();
            long resultEndTimestamp = endResult.rawCells()[0].getTimestamp();
            if(resultStartTimestamp != startTimestamp && resultEndTimestamp != endTimestamp){
                startTimestamp = resultStartTimestamp;
                endTimestamp = resultEndTimestamp;
                collector.emit(new Values(startTimestamp));
            }
            else{
                Utils.sleep(50);
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declare(new Fields("timestamp"));
    }
}
