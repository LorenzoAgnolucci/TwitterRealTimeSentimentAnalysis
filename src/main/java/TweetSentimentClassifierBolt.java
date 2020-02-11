import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class TweetSentimentClassifierBolt extends BaseRichBolt{

    private OutputCollector collector;
    private String classifierModelPath;
    private SentimentClassifier classifier;

    public TweetSentimentClassifierBolt(String classifierModelPath){
        this.classifierModelPath = classifierModelPath;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector){
        this.collector = outputCollector;
        File modelFile = new File(classifierModelPath);
        try{
            classifier = new SentimentClassifier(modelFile);
        }catch(IOException e){
            e.printStackTrace();
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(Tuple tuple){
        String text = (String) tuple.getValueByField("text");
        ArrayList<String> keywords = (ArrayList<String>) tuple.getValueByField("keywords");
        String sentiment = classifier.classify(text);
        for(String keyword: keywords){
            collector.emit(new Values(keyword, sentiment));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declare(new Fields("keyword", "sentiment"));
    }
}