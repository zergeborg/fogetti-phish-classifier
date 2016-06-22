package fogetti.phish.storm.integration;

import java.util.Properties;

import org.apache.storm.generated.StormTopology;

import redis.embedded.RedisServer;

public class TopologyEnd2End {

	public static void main(String[] args) throws Exception {
		RedisServer server = new RedisServer();
		try {
		    server.start();
    		String countDataFile = "/Users/fogetti/Work/fogetti-phish-storm/src/main/resources/1gram-count.txt";
    		String psDataFile = "/Users/fogetti/Work/fogetti-phish-storm/src/main/resources/public-suffix-list.dat";
            String proxyDataFile = "/Users/fogetti/Work/fogetti-phish-ansible/phish-storm/working-proxies.txt";
    		String modelDataFile = "/Users/fogetti/Work/backup/weka-2016-06-12/random-forest.model";
            String instancesDataFile = "/Users/fogetti/Work/backup/weka-2016-06-12/labeled-instances.csv";
            String kafkaTopicResponse = "phish-storm-response";
            String kafkaBrokerHost = "localhost";
            int kafkaBrokerPort = 9092;
            String redisHost = "localhost";
            int redisPort = 6379;
            String redisPassword = null;
            Properties kafkaSpoutProps = buildSpoutProps(kafkaBrokerHost, kafkaBrokerPort);
            Properties kafkaBoltProps = buildBoltProps(kafkaBrokerHost, kafkaBrokerPort);
    		StormTopology topology = PhishTopologyBuilder.build(
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
    		        kafkaBoltProps,
    		        null,
    		        null);
    		PhishTopologyLocalRunner.run(args, topology);
		} finally {
		    Thread.currentThread().join();
		    server.stop();
		}
	}

    private static Properties buildSpoutProps(String kafkaBrokerHost, int kafkaBrokerPort) {
        String kafkaTopicRequest = "phish-storm-request";
        String kafkaSpoutClientId = "phish-storm-client";
        int kafkaConsumerPartitionNr = 0;
        int kafkaBrokerConnectionTimeoutMs = 100000;
        String kafkaZkconnection = "localhost:2181";
        int kafkaZkconnectionTimeoutMs = 1000000;
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

    private static Properties buildBoltProps(String kafkaBrokerHost, int kafkaBrokerPort) {
        Properties kafkaBoltProps = new Properties();
        kafkaBoltProps.put("bootstrap.servers", kafkaBrokerHost+":"+kafkaBrokerPort);
        kafkaBoltProps.put("acks", "1");
        kafkaBoltProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaBoltProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return kafkaBoltProps;
    }
    
}
