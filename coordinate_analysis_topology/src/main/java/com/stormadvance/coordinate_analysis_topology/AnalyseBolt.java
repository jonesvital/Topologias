package com.stormadvance.coordinate_analysis_topology;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.json.JSONObject;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

public class AnalyseBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;
	
	boolean possuiIntercessao;
	
	Double latitude;
	Double longitude;
	
	String sentence;
	String nomeArquivo;
	
	JSONObject jSentence;
	
	Geometry geom;
	Geometry pointBuffer;
	
	Point point;
	Coordinate coord;
	GeometryFactory geometryFactory;
	SimpleFeatureCollection sfc;
	SimpleFeatureIterator sfi;
	SimpleFeature feature;
	
	public void prepare(Map stormConf, TopologyContext context){
		this.geometryFactory = JTSFactoryFinder.getGeometryFactory();

		File shapePistas = new File(stormConf.get("caminho_shape").toString());
		
		try {
			FileDataStore store = FileDataStoreFinder.getDataStore(shapePistas);
			SimpleFeatureSource source = store.getFeatureSource();
		    this.sfc = source.getFeatures();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void execute(Tuple input, BasicOutputCollector collector) {
        sentence = input.getStringByField("value");        
        jSentence = new JSONObject(sentence);                
        
        nomeArquivo = jSentence.getString("nome_imagem");
        latitude = Double.parseDouble(jSentence.getString("latitude"));
        longitude = Double.parseDouble(jSentence.getString("longitude"));
        
		coord = new Coordinate(longitude, latitude);
        point = this.geometryFactory.createPoint(coord);
        pointBuffer = point.buffer(0.003, 8);
        sfi = sfc.features();
        possuiIntercessao = false;
        
        try {
        	
			while(sfi.hasNext() && !possuiIntercessao) {
				feature = sfi.next();		
				geom = (Geometry)feature.getDefaultGeometry();
				
				if(pointBuffer.intersects(geom)) {
					possuiIntercessao = true;
				}
				    	
			}
			
        } catch (Exception e) {
//			nothing to do
		} finally {
			sfi.close();
		}
        
        if(!possuiIntercessao) {
			collector.emit(new Values(nomeArquivo, latitude, longitude));
		}
    }
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("nome_imagem", "latitude", "longitude"));
	}

}
