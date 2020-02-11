import jodd.util.StringUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.Map;

public class TweetParserBolt extends BaseRichBolt{

    private OutputCollector collector;
    String[] keywords;

    public TweetParserBolt(String[] keywords){
        this.keywords = keywords;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector){
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple){
        Status tweet = (Status) tuple.getValueByField("tweet");
        ArrayList<String> tweetKeywords = new ArrayList<String>();
        for(String keyword : keywords){
            if(tweet.getText().contains(keyword) || tweet.getText().contains(StringUtil.uncapitalize(keyword))){
                tweetKeywords.add(keyword);
            }
        }
        if(tweetKeywords.size() > 0){
            collector.emit(new Values(String.valueOf(tweet.getId()), tweet.getText(), tweetKeywords));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declare(new Fields("tweet_ID", "text", "keywords"));
    }
}
