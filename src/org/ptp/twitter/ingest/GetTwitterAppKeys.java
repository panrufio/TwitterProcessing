package org.ptp.twitter.ingest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

import javax.xml.crypto.dsig.SignatureMethod;

import oauth.signpost.OAuth;
import oauth.signpost.OAuthConsumer;
import oauth.signpost.OAuthProvider;
import oauth.signpost.basic.DefaultOAuthConsumer;
import oauth.signpost.basic.DefaultOAuthProvider;

public class GetTwitterAppKeys {

	public static void main(String[] args) throws Exception {

		String twitterKey = null;
		String twitterSecret = null;
		String twitterURL = "https://stream.twitter.com/1.1/statuses/sample.json?delimited=length";
		String config = null;
		
		for(int x = 0; x < args.length; x++){
			if(args[x].equals("-c")){
				config = args[x+1];
			}
		}
		
		File configFile = new File(config);
		if(!configFile.exists() || configFile.isDirectory()){
			config = null;
		}
		
		if(config == null){
			System.out.println("Must set -c configFile ");
			System.exit(-1);
		}

		// read in the config
		Properties props = new Properties();
		InputStreamReader inStrm = new InputStreamReader(new FileInputStream(configFile));
		props.load(inStrm);
		inStrm.close();
		twitterKey = props.getProperty("twitterKey");
		twitterSecret = props.getProperty("twitterSecret");

		System.out.println("Using: ");
		System.out.println("\ttwitterKey:               " + twitterKey);
		System.out.println("\ttwitterSecret:            " + twitterSecret);
		
		OAuthConsumer consumer = new DefaultOAuthConsumer(
				twitterKey,
				twitterSecret);
		
		OAuthProvider provider = new DefaultOAuthProvider(
				"https://api.twitter.com/oauth/request_token",
				"https://api.twitter.com/oauth/access_token",
				"https://api.twitter.com/oauth/authorize");

		System.out.println("Fetching request token from Twitter...");

		// we do not support callbacks, thus pass OOB
		String authUrl = provider.retrieveRequestToken(consumer, OAuth.OUT_OF_BAND);
		
		System.out.println("Request token: " + consumer.getToken());
		System.out.println("Token secret: " + consumer.getTokenSecret());
		System.out.println("Now visit:\n" + authUrl + "\n... and grant this app authorization");
		System.out.println("Enter the PIN code and hit ENTER when you're done:");

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String pin = br.readLine();
		
		System.out.println("Fetching access token from Twitter...");
		
		provider.retrieveAccessToken(consumer, pin);

		System.out.println("Access token: " + consumer.getToken());
		System.out.println("Token secret: " + consumer.getTokenSecret());

		//URL url = new URL("http://twitter.com/statuses/mentions.xml");
		URL url = new URL(twitterURL);

		// open the connection
		HttpURLConnection request = (HttpURLConnection) url.openConnection();
		
		//consumer.setTokenWithSecret(arg0, arg1);
		consumer.sign(request);
		
		System.out.println("Sending request to Twitter...");
		
		// connect to the url
		request.connect();

		System.out.println("Response: " + request.getResponseCode() + " " + request.getResponseMessage());

		// set up the connection reader
		InputStream ins = request.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(ins));

		int cnt = 0;
		String str;
		while((str = reader.readLine()) != null){
			System.out.println(cnt + ": " + str);
			cnt++;
			if(cnt > 10){
				break;
			}
		}
		
	} // end main

} // end BasicTwitterConnection
