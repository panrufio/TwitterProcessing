package org.ptp.twitter.ingest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import oauth.signpost.OAuthConsumer;
import oauth.signpost.OAuthProvider;
import oauth.signpost.basic.DefaultOAuthConsumer;
import oauth.signpost.basic.DefaultOAuthProvider;
import oauth.signpost.signature.OAuthMessageSigner;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class TwitterConnect {

	private static Logger log = Logger.getLogger(TwitterConnect.class);
	
	
	public static void showUse(String err){
		
	} // end showUse
	
	public static void main(String[] args) throws Exception{
		
		System.out.println("Hello Twitter World!");
		
		// curl https://stream.twitter.com/1/statuses/sample.json?delimited=length -uAnyTwitterUser:Password
		
		//String twitterURL = "https://stream.twitter.com/1/statuses/sample.json?delimited=length";
		String twitterURL = "https://stream.twitter.com/1.1/statuses/sample.json";
		
		String outDirStr = "/Users/andrew/devel/data/twitter/";
		String fileNamePrefix = "twitter-";
		String config = null;
		String twitterKey = null;
		String twitterSecret = null;
		String twitterAccessToken = null;
		String twitterAccessTokenSecret = null;
		//String twitterTokenPIN = null;
		
		for(int x = 0; x < args.length; x++){
			if(args[x].equals("-o")){
				outDirStr = args[x+1];
			} else if(args[x].equals("-c")){
				config = args[x+1];
			}
		}

		File configFile = new File(config);
		if(!configFile.exists() || configFile.isDirectory()){
			config = null;
		}
		
		if(config == null ||
				outDirStr == null){
			System.out.println("Must set -c configFile -o outputDir");
			System.exit(-1);
		}
		
		if(!outDirStr.endsWith(File.separator)){
			outDirStr += File.separator;
		}
		File outDir = new File(outDirStr);
		if(!outDir.exists()){
			outDir.mkdirs();
		}

		// read in the config
		Properties props = new Properties();
		InputStreamReader inStrm = new InputStreamReader(new FileInputStream(configFile));
		props.load(inStrm);
		inStrm.close();
		twitterKey = props.getProperty("twitterKey");
		twitterSecret = props.getProperty("twitterSecret");
		twitterAccessToken = props.getProperty("twitterAccessToken");
		twitterAccessTokenSecret = props.getProperty("twitterAccessTokenSecret");

		System.out.println("Using: ");
		System.out.println("\ttwitterKey:               " + twitterKey);
		System.out.println("\ttwitterSecret:            " + twitterSecret);
		System.out.println("\ttwitterAccessToken:       " + twitterAccessToken);
		System.out.println("\ttwitterAccessTokenSecret: " + twitterAccessTokenSecret);
		
		
//		twitterKey = "4FJG0hYbUbDum35Y21yUM79eu";
//		twitterSecret = "IbFXrBkL9NWyHOqcBfU0z1ZOsj70HOd8cryv73WJEdjvHEBoDt";
//		twitterAccessToken = "18513801-vNROuKQQW1o2RnmyKg9ihzZoKh4SQ6gTTuoqXqfLt";
//		twitterAccessTokenSecret = "KxAI2NCdoZPesqBXPgxmsuNQoZ8Ea8Tm5DVwKRZpzJxeo";
//		twitterTokenPIN = "4860444";
		
		OAuthConsumer consumer = new DefaultOAuthConsumer(twitterKey, twitterSecret);
		

		java.lang.System.setProperty("sun.security.ssl.allowUnsafeRenegotiation", "true"); 
		URL url = new URL(twitterURL);
		
		HttpsURLConnection urlCon = (HttpsURLConnection)url.openConnection();
		urlCon.setUseCaches(false);
		
		urlCon.setReadTimeout(60000);
		consumer.setTokenWithSecret(twitterAccessToken, twitterAccessTokenSecret);
		consumer.sign(urlCon);
		
		InputStream ins = urlCon.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(ins, "UTF8"));
		
		File outFile = new File(outDirStr + fileNamePrefix + System.currentTimeMillis());
		FileWriter fw = new FileWriter(outFile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		int cnt = 0;
		String str;
		while((str = reader.readLine()) != null){
			str = str.trim();
			if(cnt >= 50001){
				System.out.println();
				bw.close();
				fw.close();
				
				outFile = new File(outDirStr + fileNamePrefix + System.currentTimeMillis());
				fw = new FileWriter(outFile.getAbsoluteFile());
				bw = new BufferedWriter(fw);
				
				cnt = 0;
			}
			
			if(! str.startsWith("{")){
				continue;
			}
			
			bw.write(str + "\n");
			cnt++;
			
			
			if(cnt != 0 && (cnt % 100) == 0){
				System.out.print(".");
			}
			if(cnt != 0 && (cnt % 5000) == 0){
				System.out.println();
			}
			
		}
		
		bw.close();
		fw.close();
		reader.close();
		
	} // end main
	
	
	
	
	static class MyAuthenticator extends Authenticator{
		public String userName;
		public String passWord;
		public MyAuthenticator(String u, String p){
			userName = u;
			passWord = p;
		}
		
		public PasswordAuthentication getPasswordAuthentication(){
			
			return (new PasswordAuthentication(userName, passWord.toCharArray()));
		}
		
	}
	
	
} // end testConnect
