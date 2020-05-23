package com.stormadvance.image_analysis_topology;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class HaarBolt extends ShellBolt implements IRichBolt{
	
	public HaarBolt() {
//		super("python3", "/home/administrador/Documents/PUC/13 - TCC/Topologias/image_analysis_topology/resources/HaarBoltPython.py");
		//produção
		super("python3", "/home/administrador/resources/HaarBoltPython.py");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("nome_arquivo", "projection_ref", "xOrigin", "yOrigin", "pXh", "pXw", "pX", "pY"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
