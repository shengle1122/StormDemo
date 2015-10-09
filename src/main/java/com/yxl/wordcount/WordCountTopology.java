package com.yxl.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.yxl.wordcount.bolt.CounterBolt;
import com.yxl.wordcount.bolt.WordSplitterBolt;
import storm.kafka.*;

import java.util.Arrays;

/**
 * 读取kafka里的数据并计数
 * author: xiaolong.yuanxl
 * date: 2015-10-08 下午5:06
 */
public class WordCountTopology {

    private static final String zks = "localhost:2181";

    private static final String topic = "stormTest";

    private static final String zkRoot = "/stormTest"; // default zookeeper root configuration for storm

    private static final String id = "MyWordCount";

    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", getKafkaSpout(), 2); // Kafka我们创建了一个2分区的Topic，这里并行度设置为2
        builder.setBolt("word-splitter", new WordSplitterBolt(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new CounterBolt()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = WordCountTopology.class.getSimpleName();
//        if (args != null && args.length > 0) {
            // Nimbus host name passed from command line
            conf.put(Config.NIMBUS_HOST, "localhost");
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
//        } else {
//            conf.setMaxTaskParallelism(3);
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology(name, conf, builder.createTopology());
//            Thread.sleep(60000);
//            cluster.shutdown();
//        }
    }

    private static IRichSpout getKafkaSpout(){
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = true;
        spoutConf.zkServers = Arrays.asList(new String[]{"localhost"});
        spoutConf.zkPort = 2181;
        return new KafkaSpout(spoutConf);
    }


}
