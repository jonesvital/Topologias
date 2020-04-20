package com.stormadvance.haar_cascade_topology;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;


public class Topologia {
	public static void main(String args[]) throws Exception {
		
		Map env = new HashMap();
//		env.put("PYTHONPATH", "/home/administrador/TCC-Puc/haar_cascade_topology/resources/");
		//produção
		env.put("PYTHONPATH", "/home/administrador/resources/");
		
		HistBolt histBolt = new HistBolt();
		histBolt.setEnv(env);
		
		HaarBolt haarBolt = new HaarBolt();
		haarBolt.setEnv(env);
		
		CalcCoordsBolt calcCoordsBolt = new CalcCoordsBolt();
		calcCoordsBolt.setEnv(env);
		
		TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RecSpout", new RecSpout(), 5);
        builder.setBolt("HistBolt", histBolt, 5).shuffleGrouping("RecSpout");
        builder.setBolt("HaarBolt", haarBolt, 5).shuffleGrouping("HistBolt");
        builder.setBolt("CalcCoordsBolt", calcCoordsBolt, 3).shuffleGrouping("HaarBolt");
//        builder.setBolt("PrintBolt", new PrintBolt()).shuffleGrouping("CalcCoordsBolt");      
        builder.setBolt("SendBolt", new SendBolt(),2).shuffleGrouping("CalcCoordsBolt");
        
        Config conf = new Config();
        conf.setNumWorkers(5);
        conf.setDebug(false);
        
        StormSubmitter.submitTopology("HaarPythonTopology", conf, builder.createTopology());
        
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("HaarPythonTopology", conf, builder.createTopology());
//        Thread.sleep(1200*1000);
//        cluster.shutdown();
	}
}
