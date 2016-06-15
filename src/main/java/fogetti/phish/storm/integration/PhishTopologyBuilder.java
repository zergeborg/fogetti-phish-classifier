package fogetti.phish.storm.integration;

import static fogetti.phish.storm.relatedness.URLSpout.SUCCESS_STREAM;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import fogetti.phish.storm.client.WrappedRequest;
import fogetti.phish.storm.relatedness.ClientBuildingGoogleSemBolt;
import fogetti.phish.storm.relatedness.MatcherBolt;
import fogetti.phish.storm.relatedness.URLBolt;
import fogetti.phish.storm.relatedness.URLSpout;
import fogetti.phish.storm.relatedness.intersection.ClassifierBolt;
import fogetti.phish.storm.relatedness.intersection.IntersectionBolt;

public class PhishTopologyBuilder {

    public static final String REDIS_SEGMENT_PREFIX = "segment:";
    
    // DEFAULTS
    private static String countDataFile = System.getProperty("count.data.file");
    private static String psDataFile = System.getProperty("ps.data.file");
    private static String proxyDataFile = System.getProperty("proxy.data.file");
    private static String modelDataFile = System.getProperty("model.data.file");
    private static String instancesDataFile = System.getProperty("instances.data.file");
    private static String kafkaTopicRequest = "phish-storm-request";
    private static String kafkaTopicResponse = "phish-storm-response";
    private static String kafkaSpoutClientId = "phish-storm-client";
    private static int kafkaConsumerPartitionNr = 0;
    private static String kafkaBrokerHost = "localhost";
    private static int kafkaBrokerPort = 9092;
    private static int kafkaBrokerConnectionTimeoutMs = 100000;
    private static String kafkaZkconnection = "localhost:2181";
    private static int kafkaZkconnectionTimeoutMs = 1000000;
    private static String redisHost = "petrucci";
    private static int redisPort = 6379;
    private static String redisPassword = "Macska12";
    
	public static StormTopology build() throws Exception {
        Properties kafkaSpoutProps = buildSpoutProps();
        Properties kafkaBoltProps = buildBoltProps();
		return build(
		        countDataFile,
		        psDataFile,
		        proxyDataFile,
		        modelDataFile,
		        instancesDataFile,
		        redisHost,
		        redisPort,
		        redisPassword,
		        kafkaTopicResponse,
		        kafkaSpoutProps,
		        kafkaBoltProps);
	}

    private static Properties buildSpoutProps() {
        Properties kafkaSpoutProps = new Properties();
        kafkaSpoutProps.put("zookeeper.connect.string", kafkaZkconnection);
        kafkaSpoutProps.put("zookeeper.connection.timeout.ms", kafkaZkconnectionTimeoutMs);
        kafkaSpoutProps.put("kafka.consumer.topic", kafkaTopicRequest);
        kafkaSpoutProps.put("kafka.consumer.client.id", kafkaSpoutClientId);
        kafkaSpoutProps.put("kafka.broker.host", kafkaBrokerHost);
        kafkaSpoutProps.put("kafka.broker.port", kafkaBrokerPort);
        kafkaSpoutProps.put("kafka.broker.connection.timeout.ms", kafkaBrokerConnectionTimeoutMs);
        kafkaSpoutProps.put("kafka.consumer.partition.nr", kafkaConsumerPartitionNr);
        return kafkaSpoutProps;
    }

    private static Properties buildBoltProps() {
        Properties kafkaBoltProps = new Properties();
        kafkaBoltProps.put("bootstrap.servers", kafkaBrokerHost+":"+kafkaBrokerPort);
        kafkaBoltProps.put("acks", "1");
        kafkaBoltProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaBoltProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return kafkaBoltProps;
    }

	public static StormTopology build(
	        String countDataFile,
	        String psDataFile,
	        String proxyDataFile,
	        String modelDataFile,
	        String instancesDataFile,
            String redishost,
	        Integer redisport,
	        String redispword,
	        String kafkaTopicResponse,
	        Properties kafkaSpoutProps,
	        Properties kafkaBoltProps) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
	        .setHost(redishost).setPort(redisport).setPassword(redispword).build();
		builder
			.setSpout("urlsource", buildURLSpout(kafkaSpoutProps), 1)
			.setMaxSpoutPending(150);
		builder.setBolt("classifier", buildClassifierBolt(poolConfig, modelDataFile, instancesDataFile), 1)
		    .fieldsGrouping("urlsource", SUCCESS_STREAM, new Fields("url"))
		    .setNumTasks(1);
		builder.setBolt("kafkawriter", buildKafkaBolt(kafkaBoltProps, kafkaTopicResponse), 1)
		    .shuffleGrouping("classifier")
		    .setNumTasks(1);
		builder.setBolt("urlbolt", new URLBolt(), 1)
		    .fieldsGrouping("urlsource", new Fields("str"))
		    .setNumTasks(1);
        builder.setBolt("urlmatch", new MatcherBolt(countDataFile, psDataFile, poolConfig), 1)
            .fieldsGrouping("urlbolt", new Fields("url"))
            .setNumTasks(1);
		builder.setBolt("googletrends", new ClientBuildingGoogleSemBolt(poolConfig, new File(proxyDataFile), new WrappedRequest()), 8)
		    .addConfiguration("timeout", 15000)
            .fieldsGrouping("urlmatch", new Fields("word", "url"))
			.setNumTasks(8);
		builder.setBolt("intersection", intersectionBolt(poolConfig), 1)
			.globalGrouping("googletrends")
			.setNumTasks(1);
		StormTopology topology = builder.createTopology();
		return topology;
	}

    private static URLSpout buildURLSpout(Properties kafkaProps) {
        URLSpout spout = new URLSpout(kafkaProps, new StringScheme());
        return spout;
    }

    private static KafkaBolt<String, String> buildKafkaBolt(Properties props, String kafkaTopicResponse) {
        KafkaBolt<String, String> kafkabolt = new KafkaBolt<String, String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(kafkaTopicResponse))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>());
        return kafkabolt;
    }

    private static ClassifierBolt buildClassifierBolt(JedisPoolConfig poolConfig, String modelpath, String instancesPath) throws IOException {
        return new ClassifierBolt(poolConfig, modelpath, instancesPath);
    }

	private static IntersectionBolt intersectionBolt(JedisPoolConfig poolConfig) throws Exception {
		IntersectionBolt callback = new IntersectionBolt(poolConfig);
		return callback;
	}

}