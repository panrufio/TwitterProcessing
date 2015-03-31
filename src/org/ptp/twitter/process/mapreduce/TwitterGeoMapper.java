package org.ptp.twitter.process.mapreduce;

import java.io.IOException;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.opengis.feature.simple.SimpleFeature;
import org.ptp.twitter.process.TwitterGeoItem;
import org.ptp.twitter.process.TwitterSchema;

public class TwitterGeoMapper extends Mapper<LongWritable, Text, GeoWaveOutputKey, Object>{

	protected TwitterSchema ts = null; // new TwitterSchema();
	protected FeatureDataAdapter adapter = null; // ts.getFeatureDataAdapter();
	protected ByteArrayId adapterId = null;
	protected GeoWaveOutputKey outKey = null; // new GeoWaveOutputKey(adapterId, index.getId());
	protected Index index = null;
	
	public void setup(Context context){
		index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		
		ts = new TwitterSchema();
		adapter = ts.getFeatureDataAdapter();
		adapterId = adapter.getAdapterId();
		outKey = new GeoWaveOutputKey(adapterId, index.getId());

		
	} // end setup
	public void cleanup(Context context){} // end cleanup
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		TwitterGeoItem tgi = new TwitterGeoItem(value.toString());
		if(! tgi.hasGeo()){
			return;
		}
		SimpleFeature sf = tgi.getSimpleFeature();
		context.write(outKey, sf);
		
	} // end map
	
} // end TwitterGeoMapper
