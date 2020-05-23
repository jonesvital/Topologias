package com.stormadvance.image_analysis_topology;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class CalcCoordsBolt extends ShellBolt implements IRichBolt{
	
	public CalcCoordsBolt() {
//		super("python3", "/home/administrador/Documents/PUC/13 - TCC/Topologias/image_analysis_topology/resources/CalcCoordinatesBoltPython.py");
		//produção
		super("python3", "/home/administrador/resources/CalcCoordinatesBoltPython.py");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("nome_arquivo", "latitude", "longitude"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
