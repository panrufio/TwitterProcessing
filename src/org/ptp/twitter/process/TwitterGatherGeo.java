package org.ptp.twitter.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

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
		int fcnt = 0;
		for(String l : list){
			fcnt++;
			String curFileStr = inDirStr + l;
			System.out.println("(" + fcnt + ") " + curFileStr);
			InputStream is = new FileInputStream(new File(curFileStr));
			GZIPInputStream gis = null;
			InputStreamReader isr = null;
			BufferedReader r = null;
			if(curFileStr.endsWith("gz")){
				//System.out.println("GZIP");
				gis = new GZIPInputStream(is);
				isr = new InputStreamReader(gis);
			} else {
				isr = new InputStreamReader(is);
			}
			BufferedReader br = new BufferedReader(isr);
			
			
			String line;
			int curFileLnCnt = 0;
			while((line = br.readLine()) != null){
				curFileLnCnt++;
				cntLines++;
				if(curFileLnCnt % 500 == 0){
					System.out.print(".");
				}
				
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
			System.out.println();
			br.close();
			isr.close();
			if(curFileStr.endsWith("gz")){
				gis.close();
			}
			is.close();
			
		}
		bw.close();
		fw.close();
		System.out.println("Num lines = " + cntLines);
		System.out.println("Num geo lines = " + cntGeo);
		
		
	} // end main
	
} // end TwitterGatherGeo
