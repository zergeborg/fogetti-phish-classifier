package fogetti.phish.storm.relatedness;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.ExponentialBackoffMsgRetryManager;
import org.apache.storm.kafka.FailedMsgRetryManager;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaUtils;
import org.apache.storm.kafka.Partition;
import org.apache.storm.kafka.TopicOffsetOutOfRangeException;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.Scheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fogetti.phish.storm.relatedness.KafkaSpout.KafkaMessageId.KAFKA_MESSAGE_TYPE;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

/**
 * Kafka message consuming spout 
 * 
 * @author Gergely Nagy
 */
public class KafkaSpout extends BasicSchemeSpout {
    
    public static class KafkaMessageId implements Serializable {
        
        private static final long serialVersionUID = 6646846771919861112L;

        public static enum KAFKA_MESSAGE_TYPE {
            GOOGLE_TREND,
            CLASSIFIER
        }
        
        public long offset;
        public String value;
        public KAFKA_MESSAGE_TYPE type;

        public KafkaMessageId(long offset, KAFKA_MESSAGE_TYPE type) {
            this.offset = offset;
            this.type = type;
        }
    }

    static class ByteBufferAndKafkaMessageId implements Comparable<ByteBufferAndKafkaMessageId>, Serializable {
        
        private static final long serialVersionUID = 7320285222381644135L;
        
        public final ByteBuffer buffer;
        public final KafkaMessageId msgId;
        
        private ByteBufferAndKafkaMessageId(ByteBuffer buffer, KafkaMessageId msgId) {
            this.buffer = buffer;
            this.msgId = msgId;
        }

        @Override
        public int compareTo(ByteBufferAndKafkaMessageId other) {
            return this.buffer.compareTo(other.buffer);
        }
        
    }

    private class NonBlockingConsumerIterator {

        private final SimpleConsumer consumer;
        private final KafkaConfig kafkaConfig;
        private final Partition partition;
        private Long emittedoffset;

        public NonBlockingConsumerIterator(SimpleConsumer consumer, KafkaConfig kafkaConfig, Partition partition) {
            this.consumer = consumer;
            this.kafkaConfig = kafkaConfig;
            this.partition = partition;
        }

        public void run() {
            Long offset;

            // Are there failed tuples? If so, fetch those first.
            offset = retrymgr.nextFailedMessageToRetry();
            final boolean processingNewTuples = (offset == null);
            if (processingNewTuples) {
                offset = emittedoffset;
            }
            if (offset == null) {
                offset = KafkaUtils.getOffset(consumer, kafkaConfig.topic, partition.partition, kafkaConfig);
            }
            if (emittedoffset == null) emittedoffset = 0L;

            try {
                ByteBufferMessageSet msgs = KafkaUtils.fetchMessages(kafkaConfig, consumer, partition, offset);
                if (msgs != null) {
                    for (MessageAndOffset msg : msgs) {
                        Message message = msg.message();
                        KafkaMessageId msgId = new KafkaMessageId(msg.offset(), KAFKA_MESSAGE_TYPE.GOOGLE_TREND);
                        ByteBufferAndKafkaMessageId data = new ByteBufferAndKafkaMessageId(message.payload(), msgId);
                        bufferq.put(data);
                        emittedoffset = Math.max(msg.nextOffset(), emittedoffset);
                    }
                }
            } catch (TopicOffsetOutOfRangeException e) {
                logger.error("Could not consumer Kafka messages", e);
                if (offset > emittedoffset) {
                    logger.warn("{} Using new offset: {}", partition.partition, emittedoffset);
                }
            } catch (NullPointerException e) {
                logger.error("NPE", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }                
        }
    }

    private static final long serialVersionUID = 1064554017925026658L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaSpout.class);
    private transient BlockingQueue<ByteBufferAndKafkaMessageId> bufferq;
    private transient FailedMsgRetryManager retrymgr;
    private transient NonBlockingConsumerIterator iterator;
    private Properties kafkaProps;

    public KafkaSpout(Properties kafkaProps, Scheme scheme) {
        super(scheme);
        this.kafkaProps = kafkaProps;
    }

    /**
     * Cleanup the underlying Kafka consumer.
     */
    @Override
    public void close() {
    }

    /**
     * Consume one message from the Kafka segment on the broker. As per the documentation on {@link ISpout}, this method
     * <strong>must</strong> be non-blocking if reliability measures are enabled/used, otherwise ack and fail messages
     * will be blocked as well. This current implementation is not reliable so we use a blocking consumer.
     */
    @Override
    public void nextTuple() {
        iterator.run();
        ByteBufferAndKafkaMessageId byteBuffer = bufferq.poll();
        emit(byteBuffer);
    }

    @Override
    public void ack(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        retrymgr.acked(id.offset);
    }

    @Override
    public void fail(Object msgId) {
        KafkaMessageId id = (KafkaMessageId) msgId;
        retrymgr.failed(id.offset);
    }

    /**
     * Create a Kafka consumer.
     */
    @Override
    public void open() {
        String topic = kafkaProps.getProperty("kafka.consumer.topic", "phish-storm-request");
        String clientId = kafkaProps.getProperty("kafka.consumer.client.id", "phish-storm-client");
        String brokerHost = kafkaProps.getProperty("kafka.broker.host", "localhost");
        int brokerPort = (int) kafkaProps.getOrDefault("kafka.broker.port", 9092);
        int partitionNr = (int) kafkaProps.getOrDefault("kafka.consumer.partition.nr", 0);
        int brokerTimeout = (int) kafkaProps.getOrDefault("kafka.broker.connection.timeout.ms", 100000);
        String brokerZkStr = kafkaProps.getProperty("zookeeper.connect.string", "localhost:2181");

        Broker broker = new Broker(brokerHost, brokerPort);
        Partition partition = new Partition(broker, topic, partitionNr);
        SimpleConsumer consumer = new SimpleConsumer(brokerHost, brokerPort, brokerTimeout, 64 * 1024, clientId);
        ZkHosts hosts = new ZkHosts(brokerZkStr);
        KafkaConfig kafkaConfig = new KafkaConfig(hosts, topic, clientId);
        
        bufferq = new PriorityBlockingQueue<>();
        retrymgr = new ExponentialBackoffMsgRetryManager(0, 1.0, 60 * 1000);
        iterator = new NonBlockingConsumerIterator(consumer, kafkaConfig, partition);
    }

}