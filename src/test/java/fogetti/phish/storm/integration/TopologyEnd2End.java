package fogetti.phish.storm.integration;

import org.apache.storm.generated.StormTopology;

import redis.embedded.RedisServer;

public class TopologyEnd2End {

	public static void main(String[] args) throws Exception {
		RedisServer server = new RedisServer();
		try {
		    server.start();
    		String countDataFile = "/Users/fogetti/Work/fogetti-phish-storm/src/main/resources/1gram-count.txt";
    		String psDataFile = "/Users/fogetti/Work/fogetti-phish-storm/src/main/resources/public-suffix-list.dat";
            String proxyDataFile = "/Users/fogetti/Work/fogetti-phish-ansible/input/working-proxies.txt";
    		String modelDataFile = "/Users/fogetti/Work/backup/weka-2016-06-12/random-forest.model";
            String instancesDataFile = "/Users/fogetti/Work/backup/weka-2016-06-12/labeled-instances.csv";
    		StormTopology topology = PhishTopologyBuilder.build(countDataFile, psDataFile, proxyDataFile, modelDataFile, instancesDataFile, "localhost", 6379, null);
    		PhishTopologyLocalRunner.run(args, topology);
		} finally {
		    Thread.currentThread().join();
		    server.stop();
		}
	}

}
