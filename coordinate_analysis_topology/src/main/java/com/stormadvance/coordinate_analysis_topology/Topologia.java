package com.stormadvance.coordinate_analysis_topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class Topologia {

	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Consumidor", new Consumidor());
        builder.setBolt("AnalyseBolt", new AnalyseBolt()).shuffleGrouping("Consumidor");
        builder.setBolt("Produtor", new Produtor()).shuffleGrouping("AnalyseBolt");

        boolean producao = true;
        
        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setDebug(false);
        
        if(producao) {
	        conf.put("caminho_shape", "/home/administrador/shape_pistas/pistas_pouso_4326.shp");
	        StormSubmitter.submitTopology("CoordinateAnalysisTopology", conf, builder.createTopology());
        } else {
        	conf.put("caminho_shape", "pistas_pouso_4326.shp");
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("CoordinateAnalysisTopology", conf, builder.createTopology());
	        Thread.sleep(1200*1000);
	        cluster.shutdown();
        }
	}

}
