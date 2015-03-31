package org.ptp.twitter.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

public class TwitterGeoIngest {

	public static void main(String[] args){
		
		String zookeepers = null;
		String instance = null;
		String username = null;
		String password = null;
		String namespace = null;
		String inDir = null;

		for(int x = 0; x < args.length; x++){
			if(args[x].equals("-inDir")){
				inDir = args[x+1];
				if(!inDir.endsWith(File.separator)){
					inDir += File.separator;
				}
			} else if(args[x].equals("-z")){
				zookeepers = args[x+1];
			} else if(args[x].equals("-i")){
				instance = args[x+1];
			} else if(args[x].equals("-u")){
				username = args[x+1];
			} else if(args[x].equals("-p")){
				password = args[x+1];
			} else if(args[x].equals("-n")){
				namespace = args[x+1];
			}
		} // end for

		if(inDir == null ||
				zookeepers == null ||
				instance == null ||
				username == null ||
				password == null ||
				namespace == null){
			System.out.println("must set -inDir inputDir -z zookeepers -i instance -u username -p password -n namespace");
			System.exit(-1);
		}
		
		
		File in = new File(inDir);
		if(!in.exists() || in.isFile()){
			System.out.println("must set -i inDir");
			System.exit(-1);
		}
		
		File[] listDir = in.listFiles();
		if(listDir == null || listDir.length == 0){
			System.out.println("No files to process.");
			System.exit(-1);
		}
		
		BasicAccumuloOperations bao = null;
		try{
			bao = new BasicAccumuloOperations(
					zookeepers,
					instance,
					username,
					password,
					namespace);
		} catch(AccumuloException ae){
			ae.printStackTrace();
			System.exit(-1);
		} catch(AccumuloSecurityException ase){
			ase.printStackTrace();
			System.exit(-1);
		}
		
		Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		
		DataStore geowaveDataStore = new AccumuloDataStore(
				new AccumuloIndexStore(bao),
				new AccumuloAdapterStore(bao),
				new AccumuloDataStatisticsStore(bao),
				bao);
		
		TwitterSchema ts = new TwitterSchema();
		FeatureDataAdapter fda = ts.getFeatureDataAdapter();

		int cntWithGeo = 0;
		int cntWithoutGeo = 0;
		for(int x = 0; x < listDir.length; x++){
			System.out.println(listDir[x].getPath());
		
			FileReader fr = null;
			try{
				fr = new FileReader(listDir[x]);
			} catch(FileNotFoundException fnfe){
				fnfe.printStackTrace();
				System.exit(-1);
			}
			BufferedReader br = new BufferedReader(fr);

			String line = null;
			try{
				int cnt = 0;
				while((line = br.readLine()) != null){
					TwitterGeoItem tgi = new TwitterGeoItem(line);
					
					
					if(tgi.hasGeo()){
						System.out.println(cntWithoutGeo + "\t" + cntWithGeo + "\t" + cnt + " => " + line.length());
						//System.out.println("in file line = " + cnt + " / number geo ingested = " + cntWithGeo + " / length of current line = " + line.length());
						SimpleFeature sf = tgi.getSimpleFeature();
						if(sf == null){
							System.out.println("NULL FEATURE!!!");
							System.exit(-1);
						}
						geowaveDataStore.ingest(fda, index, tgi.getSimpleFeature());
						cntWithGeo++;
					} else {
						cntWithoutGeo++;
					}
					
					
//					if(cnt > 100){
//						System.exit(0);;
//					}
					
					
					cnt++;
				}
			} catch(IOException ioe){
				ioe.printStackTrace();
			}
		
			try{
				br.close();
				fr.close();
			} catch(IOException ioe){
				ioe.printStackTrace();
				continue;
			}
		
		} // end for
		System.out.println("Number without geo:\t" + cntWithoutGeo);
		System.out.println("Number with geo:\t" + cntWithGeo);

	}
	
} // end TwitterGeoIngest
