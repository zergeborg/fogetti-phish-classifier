package fogetti.phish.storm.relatedness.intersection;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import fogetti.phish.storm.client.Terms;
import fogetti.phish.storm.relatedness.AckResult;
import redis.clients.jedis.Jedis;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

public class ClassifierBolt extends AbstractRedisBolt {

    private static final long serialVersionUID = 8869788781918623041L;
    private static final Logger logger = LoggerFactory.getLogger(ClassifierBolt.class);
    private static final String REDIS_INTERSECTION_PREFIX = "intersect:";

    private RandomForest rforest;
    private Instances instances;
    private Encoder encoder;
    private Decoder decoder;
    private ObjectMapper mapper;
    private OutputCollector collector;
    private String modelpath;
    private String instancesPath;

    public ClassifierBolt(JedisPoolConfig config, String modelpath, String instancesPath) {
        super(config);
        this.modelpath = modelpath;
        this.instancesPath = instancesPath;
    }
    
    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        this.collector = collector;
        this.mapper = new ObjectMapper();
        this.encoder = Base64.getEncoder();
        this.decoder = Base64.getDecoder();
        CSVLoader loader = new CSVLoader();
        try {
            loader.setSource(new File(instancesPath));
            this.instances = loader.getDataSet();
        } catch (Exception e) {
            logger.error("Could not load labeled instances", e);
        } 
        try {
            this.rforest = (RandomForest) weka.core.SerializationHelper.read(modelpath);
        } catch (Exception e) {
            logger.error("Could not load the serialized classifier model", e);
        } 
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));
    }

    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("url");
        String ranking = input.getStringByField("ranking");
        try (Jedis jedis = (Jedis) getInstance()) {
            AckResult result = findAckResult(url, jedis);
            URLSegments segments = findSegments(result, jedis);
            String verdict = classify(segments, result, ranking);
            if (verdict != null) {
                collector.emit(input, new Values(url, verdict));
                collector.ack(input);
            } else {
                collector.fail(input);
            }
        } catch (Exception e) {
            logger.error("Classification failed", e);
            collector.fail(input);
        }
    }

    private AckResult findAckResult(String url, Jedis jedis) {
        logger.info("Acking enqueued message [{}]", url);
        AckResult result = null;
        try {
            String message = jedis.get("acked:"+url);
            if (message != null) {
                result = mapper.readValue(message, AckResult.class);
            } else {
                logger.warn("Could not look up AckResult related to [{}]", url);
                return new AckResult();
            }
            if (result != null) {
                save(url, jedis);
                return result;
            }
        } catch (IOException e) {
            logger.warn("Could not look up AckResult related to [{}]", url);
        }
        return new AckResult();
    }
    
    private void save(String msgId, Jedis jedis) {
        String encodedURL = getEncodedURL(msgId);
        logger.info("Saving [msgId={}]", encodedURL);
        jedis.rpush("saved:"+encodedURL, encodedURL);
    }

    private String getEncodedURL(String msgId) {
        byte[] decodedURL = decoder.decode(msgId);
        String longURL = new String(decodedURL, StandardCharsets.UTF_8);
        String URL = StringUtils.substringBeforeLast(longURL, "#");
        byte[] message = URL.getBytes(StandardCharsets.UTF_8);
        String encodedURL = encoder.encodeToString(message);
        return encodedURL;
    }

    private URLSegments findSegments(AckResult result, Jedis jedis) {
        try {
            if (result != null && result.URL != null) {
                String encodedURL = encoder.encodeToString(result.URL.getBytes(StandardCharsets.UTF_8));
                String key = REDIS_INTERSECTION_PREFIX + encodedURL;
                Map<String, String> rawSegments = jedis.hgetAll(key);
                URLSegments segments = URLSegments.fromStringMap(rawSegments);
                return segments;
            }
        } catch (IOException e) {
            logger.error("Could not find saved segments", e);
        }
        return null;
    }

    private String classify(URLSegments segments, AckResult result, String ranking) throws Exception {
        if (segments != null && segments.count() != 0) {
            Map<String, Terms> MLDTermindex = segments.getMLDTerms(result);
            Map<String, Terms> MLDPSTermindex = segments.getMLDPSTerms(result);
            Map<String, Terms> REMTermindex = segments.getREMTerms(result);
            Map<String, Terms> RDTermindex = segments.getRDTerms(result);
            segments.removeIf(termEntry -> REMTermindex.containsKey(termEntry.getKey()));
            segments.removeIf(termEntry -> RDTermindex.containsKey(termEntry.getKey()));
            IntersectionResult intersection = new IntersectionResult(RDTermindex,REMTermindex,MLDTermindex,MLDPSTermindex,ranking);
            intersection.init();
            logIntersectionResult(intersection, result.URL);
            double[] dist =  buildClassificationDistribution(intersection);
            logger.info("Message classified");
            return makeVerdict(dist);
        } else {
            logger.warn("There are no segments for [{}]. Skipping intersection", result.URL);
        }
        return null;
    }

    private void logIntersectionResult(IntersectionResult intersection, String URL) {
        logger.info("[JRR={}, "
                    + "JRA={}, "
                    + "JAA={}, "
                    + "JAR={}, "
                    + "JARRD={}, "
                    + "JARREM={}, "
                    + "CARDREM={}, "
                    + "RATIOAREM={}, "
                    + "RATIORREM={}, "
                    + "MLDRES={}, "
                    + "MLDPSRES={}, "
                    + "RANKING={}, "
                    + "URL={}]",
                    intersection.JRR(),
                    intersection.JRA(),
                    intersection.JAA(),
                    intersection.JAR(),
                    intersection.JARRD(),
                    intersection.JARREM(),
                    intersection.CARDREM(),
                    intersection.RATIOAREM(),
                    intersection.RATIORREM(),
                    intersection.MLDRES(),
                    intersection.MLDPSRES(),
                    intersection.RANKING(),
                    URL);
    }

    private double[] buildClassificationDistribution(IntersectionResult intersection) throws Exception {
        ArrayList<Attribute> attributes = new ArrayList<>(Collections.list(instances.enumerateAttributes()));
        attributes.remove(12);
        
        Instance iExample = new DenseInstance(13);
        iExample.setValue(attributes.get(0), intersection.JRR());
        iExample.setValue(attributes.get(1), intersection.JRA());
        iExample.setValue(attributes.get(2), intersection.JAA());
        iExample.setValue(attributes.get(3), intersection.JAR());
        iExample.setValue(attributes.get(4), intersection.JARRD());
        iExample.setValue(attributes.get(5), intersection.JARREM());
        iExample.setValue(attributes.get(6), intersection.CARDREM());
        iExample.setValue(attributes.get(7), intersection.RATIOAREM());
        iExample.setValue(attributes.get(8), intersection.RATIORREM());
        iExample.setValue(attributes.get(9), intersection.MLDRES());
        iExample.setValue(attributes.get(10), intersection.MLDPSRES());
        iExample.setValue(attributes.get(11), intersection.RANKING() == null ? 10000000 : intersection.RANKING());

        Instances unlabeled = new Instances("phish", attributes, 10);
        unlabeled.add(iExample);
        unlabeled.setClassIndex(unlabeled.numAttributes() - 1);
        // create copy
        Instances labeled = new Instances(unlabeled);

        // label instances
        Instance instance = unlabeled.instance(0);
        double clsLabel = rforest.classifyInstance(instance);
        labeled.instance(0).setClassValue(clsLabel);

        // return labeled data
        return rforest.distributionForInstance(instance);
    }
    
    private String makeVerdict(double[] dist) {
        logger.info("Instance distribution is [{}, {}]", dist[0], dist[1]);
        String verdict = "SAFE";
        double invalid = dist[1];
        if (invalid < 0.9 && invalid > 0.1) verdict = "CAN'T DETERMINE";
        if (invalid <= 0.1) verdict = "SAFE";
        if (invalid >= 0.9) verdict = "UNSAFE";
        return verdict;
    }

}