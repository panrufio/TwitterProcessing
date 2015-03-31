package org.ptp.twitter.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

public class TwitterGeoItems {
	
	
	public static void main(String[] args){
		
		String inDir = null;
		for(int x = 0; x < args.length; x++){
			if(args[x].equals("-inDir")){
				inDir = args[x+1];
				if(!inDir.endsWith(File.separator)){
					inDir += File.separator;
				}
			}
		}
		
		if(inDir == null){
			System.out.println("must set -inDir inputDir");
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
		
		TwitterSchema ts = new TwitterSchema();
		FeatureDataAdapter fda = ts.getFeatureDataAdapter();

		
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
						SimpleFeature sf = tgi.getSimpleFeature();
					}
				}
			} catch(IOException ioe){
				ioe.printStackTrace();
				break;
			}
		}
		
	} // end main

} // end TwitterGeoItems
