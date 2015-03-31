package org.ptp.twitter.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class TwitterGatherGeo {

	public static void main(String[] args) throws Exception{
		
		String inDirStr = null;
		String outDirStr = null;
		
		for(int x = 0; x < args.length; x++){
			if(args[x].equals("-in")){
				inDirStr = args[x+1];
			} else if(args[x].equals("-out")){
				outDirStr = args[x+1];
			}
		}

		if(inDirStr == null || outDirStr == null){
			System.out.println("Must set -in inDir -out outDir");
			System.exit(-1);
		}
		if(! inDirStr.endsWith(File.separator)){
			inDirStr += File.separator;
		}
		if(! outDirStr.endsWith(File.separator)){
			outDirStr += File.separator;
		}
		
		System.out.println("Reading from: " + inDirStr);
		System.out.println("Writer to: " + outDirStr);
		File inDir = new File(inDirStr);
		File outDir = new File(outDirStr);
		if(! inDir.exists() || inDir.isFile() || ! outDir.exists() || outDir.isFile()){
			System.out.println("Must set -in inDir -out outDir");
			System.exit(-1);
		}
		
		String[] list = inDir.list();
		Arrays.sort(list);
		int cntLines = 0;
		int cntGeo = 0;
		int curCntGeo = 0;
		int curFileCnt = 10001;
		String outFilePrefix = "twitterGeoItems-";
		String outFileName = outDirStr + outFilePrefix + curFileCnt;
		FileWriter fw = new FileWriter(new File(outFileName));
		BufferedWriter bw = new BufferedWriter(fw);
		for(String l : list){
			String curFileStr = inDirStr + l;
			FileReader fr = new FileReader(new File(curFileStr));
			BufferedReader br = new BufferedReader(fr);
			String line;
			while((line = br.readLine()) != null){
				cntLines++;
				if(line.contains("coordinates") && ! line.contains("\"coordinates\":null")){
					cntGeo++;
					curCntGeo++;
					bw.write(line + "\n");
					if(curCntGeo >= 100000){
						bw.close();
						fw.close();
						
						curFileCnt++;
						outFileName = outDirStr + outFilePrefix + curFileCnt;
						fw = new FileWriter(new File(outFileName));
						bw = new BufferedWriter(fw);
						curCntGeo = 0;
					}
					
					
					
				}
			}
			br.close();
			fr.close();
			
		}
		bw.close();
		fw.close();
		System.out.println("Num lines = " + cntLines);
		System.out.println("Num geo lines = " + cntGeo);
		
		
	} // end main
	
} // end TwitterGatherGeo
