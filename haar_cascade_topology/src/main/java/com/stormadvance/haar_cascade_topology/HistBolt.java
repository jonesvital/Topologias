package com.stormadvance.haar_cascade_topology;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class HistBolt extends ShellBolt implements IRichBolt{
	
	public HistBolt() {
//		super("python3", "/home/administrador/TCC-Puc/haar_cascade_topology/resources/HistBoltPython.py");
		//produção
		super("python3", "/home/administrador/resources/HistBoltPython.py");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("nome_arquivo", "imagem", "projection_ref", "x_origin", "y_origin", "px_h", "px_w", "X0", "Y0"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
