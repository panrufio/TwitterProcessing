package org.ptp.twitter.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;

import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;

public class TwitterSchema {

	
	public static String TWITTER_PRIMARY_KEY = "id";
	public static String TWITTER_GEOMETRY_KEY = "coordinates";
	public static String TWITTER_COORDS_KEY = "coordinates";
	public static String TWITTER_COORDS_TYPE = "type";
	public static String TWITTER_COORDS_DEFAULT_TYPE = "Point";
	
	private String contributors = "contributors";
	private String coordinates = "coordinates";
	private String created_at = "created_at";
	private String delete = "delete";
	private String entities = "entities";
	private String extended_entities = "extended_entities";
	private String favorite_count = "favorite_count";
	private String favorited = "favorited";
	private String filter_level = "filter_level";
	private String geo = "geo";
	private String id = "id";
	private String id_str = "id_str";
	private String in_reply_to_screen_name = "in_reply_to_screen_name";
	private String in_reply_to_status_id = "in_reply_to_status_id";
	private String in_reply_to_status_id_str = "in_reply_to_status_id_str";
	private String in_reply_to_user_id = "in_reply_to_user_id";
	private String in_reply_to_user_id_str = "in_reply_to_user_id_str";
	private String lang = "lang";
	private String place = "place";
	private String possibly_sensitive = "possibly_sensitive";
	private String retweet_count = "retweet_count";
	private String retweeted = "retweeted";
	private String retweeted_status = "retweeted_status";
	private String source = "source";
	private String text = "text";
	private String timestamp_ms = "timestamp_ms";
	private String truncated = "truncated";
	private String user = "user";
	
	
	
	
	
	protected Hashtable<String, String> twitterProperties;
	
	
	
	
	public TwitterSchema(){
		initialize();
	} // end constructor
	
	
	
	private void initialize(){
		
		twitterProperties = new Hashtable<String, String>();
		
		twitterProperties.put(contributors, "String");
		twitterProperties.put(coordinates, "Geometry");
		twitterProperties.put(created_at, "Date");
		twitterProperties.put(delete, "String");
		twitterProperties.put(entities, "String");
		twitterProperties.put(extended_entities, "String");
		twitterProperties.put(favorite_count, "String");
		twitterProperties.put(favorited, "String");
		twitterProperties.put(filter_level, "String");
		twitterProperties.put(geo, "String");
		twitterProperties.put(id, "String");
		twitterProperties.put(id_str, "String");
		twitterProperties.put(in_reply_to_screen_name, "String");
		twitterProperties.put(in_reply_to_status_id, "String");
		twitterProperties.put(in_reply_to_status_id_str, "String");
		twitterProperties.put(in_reply_to_user_id, "String");
		twitterProperties.put(in_reply_to_user_id_str, "String");
		twitterProperties.put(lang, "String");
		twitterProperties.put(place, "String");
		twitterProperties.put(possibly_sensitive, "String");
		twitterProperties.put(retweet_count, "String");
		twitterProperties.put(retweeted, "String");
		twitterProperties.put(retweeted_status, "String");
		twitterProperties.put(source, "String");
		twitterProperties.put(text, "String");
		twitterProperties.put(timestamp_ms, "Date");
		twitterProperties.put(truncated, "String");
		twitterProperties.put(user, "String");
		
	} // end initialize
	
	public ArrayList<String> getTwitterKeys(){
		
		ArrayList<String> keys = new ArrayList<String>();
		keys.addAll(twitterProperties.keySet());
		Collections.sort(keys);
		
		return keys;
	} // end getTwitterKeys
	
	public String getTwitterClass(String k){
		return twitterProperties.get(k);
	}
	
	
	public FeatureDataAdapter getFeatureDataAdapter(){
		return new FeatureDataAdapter(getSimpleFeatureType());
	} // end getFeatureDataAdapter
	
	
	public SimpleFeatureType getSimpleFeatureType(){
		
		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder ab = new AttributeTypeBuilder();
		builder.setName("Point-twitter");
		
		// need primary field
		// need primary time

		builder.add(ab.binding(String.class).nillable(false).buildDescriptor(TWITTER_PRIMARY_KEY));
		builder.add(ab.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));
		for(String k : twitterProperties.keySet()){
			
			//Class c = twitterProperties.get(k);
			String type = twitterProperties.get(k);
			if(type.equals("Date")){
				builder.add(ab.binding(Date.class).nillable(true).buildDescriptor(k));
			} else if(type.equals("String")){
				builder.add(ab.binding(String.class).nillable(true).buildDescriptor(k));
			} else if(type.equals("Geometry")){
				builder.add(ab.binding(Geometry.class).nillable(true).buildDescriptor(k));
			}


			if(!k.equals(TWITTER_PRIMARY_KEY)){
				if(type.equals("String")){
					builder.add(ab.binding(String.class).nillable(true).buildDescriptor(k));
				}
			}
			
			
		} // end for
		
		return builder.buildFeatureType();
	} // end getFeatureDataAdapter
	
	
	public String getGeoKey(){
		return TWITTER_GEOMETRY_KEY;
	}
	
	public static void main(String[] args){
		
		String inDir = null;
		for(int x = 0; x < args.length; x++){
			if(args[x].equals("-i")){
				inDir = args[x+1];
				if(!inDir.endsWith(File.separator)){
					inDir += File.separator;
				}
			}
		} // end for

		if(inDir == null){
			System.out.println("must set -i inDir");
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
//		for(int x = 0; x < listDir.length; x++){
//			System.out.println(listDir[x].getPath());
//		}
		
		FileReader fr = null;
		try{
			fr = new FileReader(listDir[0]);
		} catch(FileNotFoundException fnfe){
			fnfe.printStackTrace();
			System.exit(-1);;
		}
		BufferedReader br = new BufferedReader(fr);
		String line = null;
		int cnt = 0;
		int geocnt = 0;
		JSONParser parser = new JSONParser();
		Hashtable<String, Integer> tags = new Hashtable<String, Integer>();
		try{
			while((line = br.readLine()) != null){
	//			System.out.print(cnt + "," + line.length());
				cnt++;
	//			if((cnt % 100) == 0){
	//				System.out.println();
	//			} else {
	//				System.out.print(" ");
	//			}
				JSONObject jels = (JSONObject)parser.parse(line);
				Set<String> keys = jels.keySet();
				for(String k : keys){
//					System.out.println("\t" + k + ": " + jels.get(k));
					
					Integer i = tags.get(k);
					if(i == null){
						i = new Integer(1);
					} else {
						i++;
					}
					tags.put(k, i);
					// coordinates -> [longitude, latitude]
					if(k.equals("coordinates")){// || k.equals("place")){
						Object v = jels.get(k);
						if(v != null && !v.toString().equals("null")){
							geocnt++;
							System.out.println(k + " (" + geocnt + ")" + ": " + jels.get(k));
						}
					}
				} // end for
				
//				if(cnt >= 50){
//					break;
//				}
	
			
			
			}
		} catch(ParseException pe){
			//pe.printStackTrace();
			
		} catch(IOException ioe){
			ioe.printStackTrace();
			System.exit(-1);
		}

		try{
			br.close();
			fr.close();
		} catch(IOException ioe){
			ioe.printStackTrace();
			System.exit(-1);
		}
		
//		ArrayList<String> allKeys = new ArrayList<String>();
//		allKeys.addAll(tags.keySet());
//		Collections.sort(allKeys);
//		System.out.println("\n\n\n\n--------");
//		for(String s : allKeys){
//			System.out.println("twitterProperties.put("+s+", String.class"+");");
//			//System.out.println("private String " + s + " = \"" + s + "\";");// + "\t" + tags.get(s).toString());
//		}
		
		
	} // end main
	
} // end TwitterSchema
