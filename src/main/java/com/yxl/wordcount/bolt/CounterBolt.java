package com.yxl.wordcount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 计数器
 *
 * author: xiaolong.yuanxl
 * date: 2015-10-08 下午5:34
 */
public class CounterBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CounterBolt.class);

    private OutputCollector collector;
    private Map<String, AtomicInteger> counterMap;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counterMap = new HashMap<String, AtomicInteger>();
    }

    public void execute(Tuple input) {
        String word = input.getString(0);
        int count = input.getInteger(1);
        LOG.info("RECV[splitter -> counter] " + word + " : " + count);
        AtomicInteger ai = this.counterMap.get(word);
        if (ai == null){
            ai = new AtomicInteger();
            this.counterMap.put(word,ai);
        }
        ai.getAndAdd(count);
        collector.ack(input);
        LOG.info("CHECK statistics map: " + this.counterMap);
    }

    @Override
    public void cleanup() {
        LOG.info("The final result:");
        Iterator<Map.Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<String, AtomicInteger> entry = iter.next();
            LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
