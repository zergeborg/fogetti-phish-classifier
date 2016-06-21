package fogetti.phish.storm.relatedness.intersection;

import org.apache.storm.tuple.Tuple;

import fogetti.phish.storm.relatedness.KafkaSpout.KafkaMessageId;

public class KafkaAlexaRankingBolt extends AlexaRankingBolt {

    private static final long serialVersionUID = 674703140284062905L;

    public KafkaAlexaRankingBolt(String proxyDataFile) {
        super(proxyDataFile);
    }

    @Override
    protected String getURL(Tuple input) {
        KafkaMessageId data = (KafkaMessageId)input.getValueByField("url");
        String URL = data.value;
        return URL;
    }
    
}