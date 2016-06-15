package fogetti.phish.storm.relatedness;

import java.util.Map;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fogetti.phish.storm.relatedness.KafkaSpout.ByteBufferAndKafkaMessageId;

public abstract class BasicSchemeSpout extends BaseRichSpout {

    private static final long serialVersionUID = -832374927567182090L;
    private static final Logger logger = LoggerFactory.getLogger(BasicSchemeSpout.class);
    private final Scheme scheme;
    private transient SpoutOutputCollector collector;

    public BasicSchemeSpout(final Scheme scheme) {
        this.scheme = scheme;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        open();
    }
    
    /**
     * Delegates to the {@link Scheme}.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(scheme.getOutputFields());
    }

    /**
     * Delegates deserialization to the {@link Scheme}.
     */
    protected void emit(final ByteBufferAndKafkaMessageId data) {
        if (data != null && data.buffer != null) {
            data.msgId.value = StringScheme.deserializeString(data.buffer);
            collector.emit(scheme.deserialize(data.buffer), data.msgId);
            logger.info("Emitted [{}]", data.msgId.value);
        }
    }
    
    protected abstract void open();
}