package com.yxl.wordcount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.StringTokenizer;

/**
 * 解析器
 *
 * author: xiaolong.yuanxl
 * date: 2015-10-08 下午5:18
 */
public class WordSplitterBolt extends BaseRichBolt{

    private static final Logger LOG = LoggerFactory.getLogger(WordSplitterBolt.class);

    private OutputCollector collector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String line = tuple.getString(0);
        LOG.info("[Receive] " + line);
        StringTokenizer st = new StringTokenizer(line);
        while(st.hasMoreTokens()){
            String word = st.nextToken();
            collector.emit(tuple, new Values(word, 1));
        }
        collector.ack(tuple);   // tell storm this tuple was handled successfully
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
