package com.stormadvance.image_analysis_topology;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Consumidor extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	KafkaConsumer<String, String> consumer = null;
    SpoutOutputCollector collector;
    Properties props = new Properties();

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        props.put("bootstrap.servers", "192.168.25.102:9092");
        props.put("group.id", "filaAnaliseImagens");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("filaAnaliseImagens"));
    }

    public void nextTuple() {
        ConsumerRecords<String, String> records = consumer.poll(500);
        for (ConsumerRecord<String, String> record : records) {
            collector.emit(new Values(record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topico", "particao", "offset", "key", "value"));
    }
}
