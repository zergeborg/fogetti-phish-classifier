package fogetti.phish.storm.relatedness;

import java.util.Map;
import java.util.Properties;

import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fogetti.phish.storm.relatedness.KafkaSpout.KafkaMessageId.KAFKA_MESSAGE_TYPE;

/**
 * Implementation of the following real time phishing classifier:
 * <a href="https://orbilu.uni.lu/bitstream/10993/20053/1/phishStorm-revised.pdf">
 * https://orbilu.uni.lu/bitstream/10993/20053/1/phishStorm-revised.pdf
 * </a>
 * @author gergely.nagy
 *
 */
public class URLSpout extends KafkaSpout {

    public static final String SUCCESS_STREAM = "success";
    public static final String INTERSECTION_STREAM = "intersect";
	
    private static final long serialVersionUID = -6424905468176142975L;
	private static final Logger logger = LoggerFactory.getLogger(URLSpout.class);
	private static final int METRICS_WINDOW = 10;
    private transient SpoutOutputCollector collector;
    private transient CountMetric spoutAcked;
    private transient CountMetric spoutFailed;
    private transient CountMetric spoutEmitted;

	public URLSpout(Properties kafkaProperties, Scheme scheme) {
	    super(kafkaProperties, scheme);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    super.open(conf, context, collector);
        this.collector = collector;
        spoutAcked = new CountMetric();
        context.registerMetric("spout-acked",
                               spoutAcked,
                               METRICS_WINDOW);
        spoutFailed = new CountMetric();
        context.registerMetric("spout-failed",
                               spoutFailed,
                               METRICS_WINDOW);
        spoutEmitted = new CountMetric();
        context.registerMetric("spout-emitted",
                               spoutEmitted,
                               METRICS_WINDOW);
	}

	@Override
	public void nextTuple() {
	    super.nextTuple();
        spoutEmitted.incr();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    super.declareOutputFields(declarer);
		declarer.declareStream(SUCCESS_STREAM, new Fields("url"));
        declarer.declareStream(INTERSECTION_STREAM, new Fields("url"));
	}

	@Override
	public void ack(Object msgId) {
	    KafkaMessageId id = (KafkaMessageId) msgId;
	    if (id.type == KAFKA_MESSAGE_TYPE.GOOGLE_TREND) {
	        super.ack(msgId);
    	    id.type = KAFKA_MESSAGE_TYPE.SEGMENT_SAVING;
    	    collector.emit(INTERSECTION_STREAM, new Values(id), id);
	    }
        if (id.type == KAFKA_MESSAGE_TYPE.SEGMENT_SAVING) {
            id.type = KAFKA_MESSAGE_TYPE.CLASSIFIER;
            collector.emit(SUCCESS_STREAM, new Values(id), id);
        }
        logger.info("Acking [{}]", msgId);
        spoutAcked.incr();
	}

	@Override
	public void fail(Object msgId) {
	    super.fail(msgId);
		logger.debug("Message [msg={}] failed", msgId.toString());
		spoutFailed.incr();
	}

}