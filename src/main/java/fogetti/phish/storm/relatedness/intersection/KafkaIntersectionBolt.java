package fogetti.phish.storm.relatedness.intersection;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.tuple.Tuple;

import fogetti.phish.storm.relatedness.KafkaSpout.KafkaMessageId;

public class KafkaIntersectionBolt extends IntersectionBolt {

    private static final long serialVersionUID = 6092407511669593227L;

    public KafkaIntersectionBolt(JedisPoolConfig config) {
        super(config);
    }
    
    @Override
    protected String getInputURL(Tuple input) {
        KafkaMessageId data = (KafkaMessageId)input.getValueByField("url");
        String url = data.value;
        return url;
    }


}