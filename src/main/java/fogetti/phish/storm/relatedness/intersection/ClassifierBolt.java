package fogetti.phish.storm.relatedness.intersection;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
import fogetti.phish.storm.client.WrappedRequest;
import fogetti.phish.storm.relatedness.AckResult;
import fogetti.phish.storm.relatedness.KafkaSpout.KafkaMessageId;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
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
    private int connectTimeout = 15000;
    private int socketTimeout = 15000;
    private String modelpath;
    private String instancesPath;
    private String proxyDataFile;
    private List<String> proxyList;

    public ClassifierBolt(JedisPoolConfig config, String modelpath, String instancesPath, String proxyDataFile) {
        super(config);
        this.modelpath = modelpath;
        this.instancesPath = instancesPath;
        this.proxyDataFile = proxyDataFile;
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
        try {
            this.proxyList = Files.readAllLines(Paths.get(proxyDataFile));
        } catch (IOException e) {
            logger.error("Preparing the Google SEM bolt failed", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));
    }

    @Override
    public void execute(Tuple input) {
        KafkaMessageId data = (KafkaMessageId)input.getValueByField("url");
        String url = data.value;
        try (Jedis jedis = (Jedis) getInstance()) {
            String encoded = encoder.encodeToString(url.getBytes(StandardCharsets.UTF_8));
            AckResult result = findAckResult(encoded);
            URLSegments segments = findSegments(result);
            String verdict = classify(segments, result);
            collector.emit(input, new Values(url, verdict.equals("yes") ? "SAFE" : "UNSAFE"));
            collector.ack(input);
        } catch (Exception e) {
            logger.error("Classification failed", e);
            collector.fail(input);
        }
    }

    private AckResult findAckResult(String url) {
        try (Jedis jedis = (Jedis) getInstance()) {
            logger.info("Acking enqueued message [{}]", url);
            AckResult result = null;
            try {
                List<String> messages = jedis.blpop(0, new String[]{"classacked:"+url});
                if (messages != null) {
                    result = mapper.readValue(messages.get(1), AckResult.class);
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

    private URLSegments findSegments(AckResult result) {
        try (Jedis jedis = (Jedis) getInstance()) {
            String encodedURL = encoder.encodeToString(result.URL.getBytes(StandardCharsets.UTF_8));
            String key = REDIS_INTERSECTION_PREFIX + encodedURL;
            Map<String, String> rawSegments = jedis.hgetAll(key);
            URLSegments segments = URLSegments.fromStringMap(rawSegments);
            return segments;
        } catch (IOException e) {
            logger.error("Could not find saved segments", e);
        }
        return null;
    }

    private String classify(URLSegments segments, AckResult result) throws Exception {
        if (segments != null) {
            Map<String, Terms> MLDTermindex = segments.getMLDTerms(result);
            Map<String, Terms> MLDPSTermindex = segments.getMLDPSTerms(result);
            Map<String, Terms> REMTermindex = segments.getREMTerms(result);
            Map<String, Terms> RDTermindex = segments.getRDTerms(result);
            segments.removeIf(termEntry -> REMTermindex.containsKey(termEntry.getKey()));
            segments.removeIf(termEntry -> RDTermindex.containsKey(termEntry.getKey()));
            OkHttpClient client = buildClient();
            IntersectionResult intersection = new IntersectionResult(RDTermindex,REMTermindex,MLDTermindex,MLDPSTermindex, new WrappedRequest(), result.URL, client);
            intersection.init();
            logIntersectionResult(intersection, result.URL);
            String verdict =  buildClassificationVerdict(intersection);
            logger.info("Message classified");
            return verdict;
        } else {
            logger.warn("There are no segments for [{}]. Skipping intersection", result.URL);
        }
        return null;
    }

    private OkHttpClient buildClient() throws UnknownHostException {
        int nextPick = new Random().nextInt(proxyList.size());
        String nextProxy = proxyList.get(nextPick);
        String[] hostAndPort = nextProxy.split(":");
        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);
        OkHttpClient client
            = new OkHttpClient
                .Builder()
                .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                .readTimeout(socketTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(socketTimeout, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(true)
                .followRedirects(true)
                .followSslRedirects(true)
                .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(InetAddress.getByName(host), port)))
                .addInterceptor(new Interceptor() {
                    @Override
                    public Response intercept(Chain chain) throws IOException {
                        Request request = chain.request();
                        // try the request
                        Response response = chain.proceed(request);
                        int tryCount = 0;
                        while (!response.isSuccessful() && tryCount < 3) {
                            logger.info("Retry request [{}] was not successful", tryCount);
                            tryCount++;
                            // retry the request
                            response = chain.proceed(request);
                        }
                        // otherwise just pass the original response on
                        return response;
                    }
                }).build();
        return client;
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

    private String buildClassificationVerdict(IntersectionResult intersection) throws Exception {
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
        double clsLabel = rforest.classifyInstance(unlabeled.instance(0));
        labeled.instance(0).setClassValue(clsLabel);

        // return labeled data
        return labeled.classAttribute().value((int)clsLabel);
    }
    
}