package com.stormadvance.haar_cascade_topology;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PrintBolt extends BaseBasicBolt{

	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println(input.getStringByField("nome_arquivo"));
		System.out.println(input.getStringByField("latitude"));
		System.out.println(input.getStringByField("longitude"));
//		System.out.println(input.getStringByField("projection_ref"));
//		System.out.println(input.getStringByField("x_origin"));
//		System.out.println(input.getStringByField("y_origin"));
//		System.out.println(input.getStringByField("px_h"));
//		System.out.println(input.getStringByField("px_w"));
//		System.out.println(input.getStringByField("pX"));
//		System.out.println(input.getStringByField("pY"));
		collector.emit(new Values(input.getStringByField("nome_arquivo"), input.getStringByField("latitude"), input.getStringByField("longitude")));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("nome_arquivo", "latitude", "longitude"));
		
	}

}
