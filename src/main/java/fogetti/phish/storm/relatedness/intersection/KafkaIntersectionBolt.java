package fogetti.phish.storm.relatedness.intersection;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import fogetti.phish.storm.relatedness.KafkaSpout.KafkaMessageId;

public class KafkaIntersectionBolt extends IntersectionBolt {

    private static final long serialVersionUID = 6092407511669593227L;
    private Encoder encoder;

    public KafkaIntersectionBolt(JedisPoolConfig config) {
        super(config);
    }
    
    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.encoder = Base64.getEncoder();
    }
    
    @Override
    protected String getInputURL(Tuple input) {
        KafkaMessageId data = (KafkaMessageId)input.getValueByField("url");
        String encodedURL = encoder.encodeToString(data.value.getBytes(StandardCharsets.UTF_8));
        String url = encodedURL;
        return url;
    }


}