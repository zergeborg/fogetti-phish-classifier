package fogetti.phish.storm.integration;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.LoggingMetricsConsumer;

public class PhishTopologyRemoteRunner {

	public static void main(String[] args) throws Exception {
        String countDataFile = args[0];
        String psDataFile = args[1];
        String proxyDataFile = args[2];
        String modelDataFile = args[3];
        String instancesDataFile = args[4];
        String kafkaTopicResponse = args[6];
        Properties kafkaSpoutProps = buildSpoutProps(args);
        Properties kafkaBoltProps = buildBoltProps(args);
        String redisHost = args[14];
        int redisPort = Integer.parseInt(args[15]);
        String redisPassword = args[16];

		StormTopology topology
		    = PhishTopologyBuilder.build(
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
		
		Config config = new Config();
		config.setNumWorkers(3);
		config.setMessageTimeoutSecs(30);
		config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,
	               new Integer(16384));
	    config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,
	               new Integer(16384));
	    config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,
	               new Integer(16384));
	    config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
		
		StormSubmitter.submitTopology("phish-classifier", config, topology);
	}

    private static Properties buildSpoutProps(String[] args) {
        Properties kafkaSpoutProps = new Properties();
        kafkaSpoutProps.put("kafka.consumer.topic", args[5]);
        kafkaSpoutProps.put("kafka.consumer.client.id", args[7]);
        kafkaSpoutProps.put("kafka.consumer.partition.nr", Integer.parseInt(args[8]));
        kafkaSpoutProps.put("kafka.broker.host", args[9]);
        kafkaSpoutProps.put("kafka.broker.port", Integer.parseInt(args[10]));
        kafkaSpoutProps.put("kafka.broker.connection.timeout.ms", Integer.parseInt(args[11]));
        kafkaSpoutProps.put("zookeeper.connect.string", args[12]);
        kafkaSpoutProps.put("zookeeper.connection.timeout.ms", Integer.parseInt(args[13]));
        return kafkaSpoutProps;
    }

    private static Properties buildBoltProps(String[] args) {
        Properties kafkaBoltProps = new Properties();
        kafkaBoltProps.put("bootstrap.servers", args[9]+":"+args[10]);
        kafkaBoltProps.put("acks", "1");
        kafkaBoltProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaBoltProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return kafkaBoltProps;
    }
	
}