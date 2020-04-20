package com.stormadvance.haar_cascade_topology;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;

public class SendBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;
	Properties props = null;
	Producer<String, String> producer = null;
	
	public void prepare(Map stormConf, TopologyContext context){
			
		props = new Properties();
		props.put("bootstrap.servers", "192.168.25.102:9092");
		props.put("transactional.id", String.valueOf(context.getThisTaskId()));
		//props.put("client.id",String.valueOf(context.getThisTaskId()));
		props.put("acks", "all");
		//props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());

		producer.initTransactions();
	}
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		JSONObject jMsg = new JSONObject();
		
		jMsg.put("nome_imagem", input.getStringByField("nome_arquivo"));
		jMsg.put("latitude" , input.getStringByField("latitude"));
		jMsg.put("longitude", input.getStringByField("longitude"));

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>("analysequeue", jMsg.toString()));
		producer.commitTransaction();
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
