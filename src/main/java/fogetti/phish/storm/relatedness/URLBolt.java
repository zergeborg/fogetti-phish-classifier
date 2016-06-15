package fogetti.phish.storm.relatedness;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLBolt extends BaseRichBolt {

    private static final long serialVersionUID = -2495671243590461377L;
    private static final Logger logger = LoggerFactory.getLogger(URLBolt.class);
    private Encoder encoder;
    private OutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.encoder = Base64.getEncoder();
    }

    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("str");
        String encodedURL = getEncodedURL(url);
        collector.emit(input, new Values(encodedURL));
        logger.info("URL [{}] was successfully emitted", encodedURL);
        collector.ack(input);
        logger.info("URL [{}] was successfully acked", encodedURL);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url"));
    }

    private String getEncodedURL(String longURL) {
        byte[] message = longURL.getBytes(StandardCharsets.UTF_8);
        String encodedURL = encoder.encodeToString(message);
        return encodedURL;
    }

}
