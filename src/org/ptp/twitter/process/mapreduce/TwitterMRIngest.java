package org.ptp.twitter.process.mapreduce;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputFormat;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ptp.twitter.process.TwitterSchema;


public class TwitterMRIngest extends Configured implements Tool{

	public int run(String[] args) throws Exception{
		
		String inDirStr = null;
		String outDirStr = null;
		String zooKeepers = null;
		String instance = null;
		String user = null;
		String password = null;
		String namespace = null;
		
		for(int x = 0; x < args.length; x++){
			if(args[x].equals("-in")){
				inDirStr = args[x+1];
			} else if(args[x].equals("-out")){
				outDirStr = args[x+1];
			} else if(args[x].equals("-z")){
				zooKeepers = args[x+1];
			} else if(args[x].equals("-i")){
				instance = args[x+1];
			} else if(args[x].equals("-u")){
				user = args[x+1];
			} else if(args[x].equals("-p")){
				password = args[x+1];
			} else if(args[x].equals("-n")){
				namespace = args[x+1];
			}
		}

		if(inDirStr == null ||
				zooKeepers == null ||
				instance == null ||
				user == null ||
				password == null ||
				namespace == null){
			showUse("Must set all variables.");
			System.exit(-1);
		}
		
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Twitter Geo Ingest");
		job.setJarByClass(TwitterMRIngest.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inDirStr);
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		job.setMapperClass(TwitterGeoMapper.class);
		job.setMapOutputKeyClass(GeoWaveOutputKey.class);
		job.setMapOutputValueClass(Object.class);
		
		GeoWaveOutputFormat.setAccumuloOperationsInfo(job.getConfiguration(),
				zooKeepers,
				instance,
				user,
				password,
				namespace);
		Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		GeoWaveOutputFormat.addIndex(job.getConfiguration(), index);
		TwitterSchema ts = new TwitterSchema();
		FeatureDataAdapter adapter = ts.getFeatureDataAdapter();
		GeoWaveOutputFormat.addDataAdapter(job.getConfiguration(), adapter);
				
		job.setNumReduceTasks(0);
		
		int retVal = job.waitForCompletion(true) ? 0 : 1;
		
		return -1;
	} // end run
	
	
	public static void showUse(String err){
		StringBuffer sb = new StringBuffer();
		sb.append("error: " + err + "\n");
		sb.append("Usage: yarn jar twitter.jar " + TwitterMRIngest.class.getCanonicalName() + " \\\n");
		sb.append("\t-in inDir \\\n");
		sb.append("\t-out outDir \\\n");
		sb.append("\t-z zookeepers \\\n");
		sb.append("\t-i instance \\\n");
		sb.append("\t-u user \\\n");
		sb.append("\t-p password \\\n");
		sb.append("\t-n namespace \n");

		
		
		System.out.println(sb.toString());
		
	} // end showUse
	
	public static void main(String[] args){
		try{
			int v = ToolRunner.run(new Configuration(), new TwitterMRIngest(), args);
		} catch(Exception e){
			e.printStackTrace();
		}
	} // end main
} // end TwitterMRIngest
