import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetStreamSpout extends BaseRichSpout{

    private TwitterStream twitterStream;
    private LinkedBlockingQueue<Status> queue = null;
    private SpoutOutputCollector collector;

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String[] keywords;

    public TweetStreamSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, String[] keywords){
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keywords = keywords;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector){
        this.collector = spoutOutputCollector;
        queue = new LinkedBlockingQueue<Status>(1000);

        StatusListener listener = new StatusListener(){
            public void onStatus(Status status){
                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice){}

            public void onTrackLimitationNotice(int i){}

            public void onScrubGeo(long l, long l1){}

            public void onStallWarning(StallWarning stallWarning){}

            public void onException(Exception e){}
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);

        this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(listener);

        if(keywords.length == 0){
            twitterStream.sample();
        }
        else{
            FilterQuery query = new FilterQuery().track(keywords);
            twitterStream.filter(query);
        }

    }

    public void nextTuple(){
        Status tweet = queue.poll();
        if(tweet == null){
            Utils.sleep(50);
        }
        else {
            if(tweet.getLang().equals("en")){
                collector.emit(new Values(tweet));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public void close(){
        twitterStream.shutdown();
    }
}
