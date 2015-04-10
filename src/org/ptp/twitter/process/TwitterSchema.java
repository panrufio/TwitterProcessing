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
	public static String TWITTER_ID_KEY = "id";
	public static String TWITTER_CREATED_TIME_KEY = "created_at";
	
	public static String TWITTER_USER_KEY = "user";
	public static String TWITTER_USER_ID_KEY = "id_str";
	public static String TWITTER_USER_NAME_KEY = "name";
	
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
	
	private String user_contributors_enabled = "contributors_enabled";
	private String user_created_at = "created_at";
	private String user_default_profile = "default_profile";
	private String user_default_profile_image = "default_profile_image";
	private String user_description = "description";
	private String user_favourites_count = "favourites_count";
	private String user_follow_request_sent = "follow_request_sent";
	private String user_followers_count = "followers_count";
	private String user_following = "following";
	private String user_friends_count = "friends_count";
	private String user_geo_enabled = "geo_enabled";
	private String user_id_str = "id_str";
	private String user_is_translator = "is_translator";
	private String user_lang = "lang";
	private String user_listed_count = "listed_count";
	private String user_location = "location";
	private String user_name = "name";
	private String user_notifications = "notifications";
	private String user_profile_background_color = "profile_background_color";
	private String user_profile_background_image_url = "profile_background_image_url";
	private String user_profile_background_image_url_https = "profile_background_image_url_https";
	private String user_profile_background_tile = "profile_background_tile";
	private String user_profile_banner_url = "profile_banner_url";
	private String user_profile_image_url = "profile_image_url";
	private String user_profile_image_url_https = "profile_image_url_https";
	private String user_profile_link_color = "profile_link_color";
	private String user_profile_sidebar_border_color = "profile_sidebar_border_color";
	private String user_profile_sidebar_fill_color = "profile_sidebar_fill_color";
	private String user_profile_text_color = "profile_text_color";
	private String user_profile_use_background_image = "profile_use_background_image";
	private String user_protected = "protected";
	private String user_screen_name = "screen_name";
	private String user_statuses_count = "statuses_count";
	private String user_time_zone = "time_zone";
	private String user_url = "url";
	private String user_utc_offset = "utc_offset";
	private String user_verified = "verified";

	
	// items for the user
	//private String
	/*
		contributors_enabled
		created_at
		default_profile
		default_profile_image
		description
		favourites_count
		follow_request_sent
		followers_count
		following
		friends_count
		geo_enabled
		id_str
		is_translator
		lang
		listed_count
		location
		name
		notifications
		profile_background_color
		profile_background_image_url
		profile_background_image_url_https
		profile_background_tile
		profile_banner_url
		profile_image_url
		profile_image_url_https
		profile_link_color
		profile_sidebar_border_color
		profile_sidebar_fill_color
		profile_text_color
		profile_use_background_image
		protected
		screen_name
		statuses_count
		time_zone
		url
		utc_offset
		verified
	 */	
	
	
	
	
	
	protected Hashtable<String, String> twitterProperties;
	protected Hashtable<String, String> twitterUserProperties;
	
	
	
	public TwitterSchema(){
		initialize();
	} // end constructor
	
	
	
	private void initialize(){
		
		twitterProperties = new Hashtable<String, String>();
		twitterUserProperties = new Hashtable<String, String>();
		
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
		
		twitterUserProperties.put(user_contributors_enabled, "String");
		twitterUserProperties.put(user_created_at, "Date");
		twitterUserProperties.put(user_default_profile, "String");
		twitterUserProperties.put(user_default_profile_image, "String");
		twitterUserProperties.put(user_description, "String");
		twitterUserProperties.put(user_favourites_count, "Integer");
		twitterUserProperties.put(user_follow_request_sent, "String");
		twitterUserProperties.put(user_followers_count, "Integer");
		twitterUserProperties.put(user_following, "String");
		twitterUserProperties.put(user_friends_count, "Integer");
		twitterUserProperties.put(user_geo_enabled, "String");
		twitterUserProperties.put(user_id_str, "String");
		twitterUserProperties.put(user_is_translator, "String");
		twitterUserProperties.put(user_lang, "String");
		twitterUserProperties.put(user_listed_count, "Integer");
		twitterUserProperties.put(user_location, "String");
		twitterUserProperties.put(user_name, "String");
		twitterUserProperties.put(user_notifications, "String");
		twitterUserProperties.put(user_profile_background_color, "String");
		twitterUserProperties.put(user_profile_background_image_url, "String");
		twitterUserProperties.put(user_profile_background_image_url_https, "String");
		twitterUserProperties.put(user_profile_background_tile, "String");
		twitterUserProperties.put(user_profile_banner_url, "String");
		twitterUserProperties.put(user_profile_image_url, "String");
		twitterUserProperties.put(user_profile_image_url_https, "String");
		twitterUserProperties.put(user_profile_link_color, "String");
		twitterUserProperties.put(user_profile_sidebar_border_color, "String");
		twitterUserProperties.put(user_profile_sidebar_fill_color, "String");
		twitterUserProperties.put(user_profile_text_color, "String");
		twitterUserProperties.put(user_profile_use_background_image, "String");
		twitterUserProperties.put(user_protected, "String");
		twitterUserProperties.put(user_screen_name, "String");
		twitterUserProperties.put(user_statuses_count, "String");
		twitterUserProperties.put(user_time_zone, "Date");
		twitterUserProperties.put(user_url, "String");
		twitterUserProperties.put(user_utc_offset, "String");
		twitterUserProperties.put(user_verified, "String");
		
	} // end initialize
	
	public ArrayList<String> getTwitterKeys(){
		
		ArrayList<String> keys = new ArrayList<String>();
		keys.addAll(twitterProperties.keySet());
		Collections.sort(keys);
		
		return keys;
	} // end getTwitterKeys
	
	public ArrayList<String> getTwitterUserKeys(){
		ArrayList<String> keys = new ArrayList<String>();
		keys.addAll(twitterUserProperties.keySet());
		Collections.sort(keys);
		
		return keys;
	} // end getTwitterUserKeys
	
	
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
