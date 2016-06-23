package fogetti.phish.storm.integration;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;

public class PhishTopologyLocalRunner {

	public static void run(String[] args, StormTopology topology)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Config conf = new Config();
		conf.setMessageTimeoutSecs(5);
		runSimply(topology, conf);
	}

	private static void runSimply(StormTopology topology, Config conf) {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, topology);
	}
}