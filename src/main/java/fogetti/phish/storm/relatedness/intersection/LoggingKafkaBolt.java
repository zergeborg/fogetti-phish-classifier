package fogetti.phish.storm.relatedness.intersection;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingKafkaBolt<K,V> extends KafkaBolt<K, V> {

    private static final long serialVersionUID = -1273479960783034875L;
    private static final Logger logger = LoggerFactory.getLogger(LoggingKafkaBolt.class);

    @Override
    public void execute(final Tuple input) {
        super.execute(input);
        logger.info("KafkaWriter finished executing [{}]", input);
    }
}