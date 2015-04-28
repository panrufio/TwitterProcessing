package org.ptp.twitter.process;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class TwitterGeoItem {

	private boolean geoEnabled = false;
	private TwitterSchema twitterSchema;
	private Geometry geo;
	private String geoType;
	private String md5;
	private JSONObject jels;
	private JSONObject joGeo;
	private JSONObject userJSON;
	
	private Properties props;
	private Properties userProps;
	
	public TwitterGeoItem(){
		twitterSchema = new TwitterSchema();
	} // end constructor
	
	public TwitterGeoItem(String line){
		twitterSchema = new TwitterSchema();
		parseItem(line);
		
	} // end constructor
	
	
	public void parseItem(String line){
		// calculate the md5 of the line
		props = new Properties();
		userProps = new Properties();

		JSONParser parser = null; //= new JSONParser();
		//JSONObject jels = null;
		try{
			parser = new JSONParser();
			jels = (JSONObject)parser.parse(line);
			JSONObject geoObj = (JSONObject)(jels.get(twitterSchema.getGeoKey()));
			if(geoObj == null){
				geo = null;
				geoEnabled = false;
				return;
			}
			parseGeo(geoObj.toJSONString());
			//parseAllItems(jels);

			userJSON = (JSONObject)(jels.get(twitterSchema.TWITTER_USER_KEY));
			parseUser(userJSON);
			
			ArrayList<String> allKeys = twitterSchema.getTwitterKeys();			
			for(String ak : allKeys){
				
				String v = "";
				Object o = jels.get(ak);
				if(o == null){
					continue;
				} else {
					v = o.toString();
					props.put(ak, v);
				}
				
			}
			
		} catch(ParseException pe){
			pe.printStackTrace();
		}
		
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(line.getBytes());
			byte[] digest = md.digest();
			StringBuffer hexString = new StringBuffer();
	    	for (int i=0;i< digest.length;i++) {
	    		String hex=Integer.toHexString(0xff & digest[i]);
	   	     	if(hex.length()==1) hexString.append('0');
	   	     	hexString.append(hex);
	    	}
			
			md5 = hexString.toString();
			
		} catch(NoSuchAlgorithmException nsae){
			nsae.printStackTrace();
			System.exit(-1);
		}
		
		
		
	} // end parseItem

	
	
	
	private void parseGeo(String gs){
		if(gs == null){
			return;
		}
		JSONParser jpGeo = null;
		joGeo = null;
		try{
			jpGeo = new JSONParser();
			joGeo = (JSONObject)jpGeo.parse(gs);
		} catch(ParseException pe){
			pe.printStackTrace();
			return;
		}
		
		// we know longitude latitude
		JSONArray coords = (JSONArray)joGeo.get(TwitterSchema.TWITTER_COORDS_KEY);
		if(coords == null){
			geoEnabled = false;
			geo = null;
			return;
		}
		Iterator<Object> it = coords.iterator();
		double lat = Double.MIN_VALUE;
		double lon = Double.MIN_VALUE;
		if(it.hasNext()){
			// longitude is first
			lon = Double.parseDouble((it.next()).toString());
		}
		if(it.hasNext()){
			// latitude is second
			lat = Double.parseDouble((it.next()).toString()) ;
		}
		if(lat == Double.MIN_VALUE ||
				lon == Double.MIN_VALUE){
			geo = null;
			geoEnabled = false;
			return;
		}
		GeometryFactory fact = JTSFactoryFinder.getGeometryFactory();
		Coordinate c = new Coordinate(lon, lat);
		geo = (Geometry)fact.createPoint(c);
		//System.out.println("Coords = " + c.toString());
		geoType = (String)joGeo.get(TwitterSchema.TWITTER_COORDS_TYPE);
		geoEnabled = true;
		
//		if(!geoType.equals(TwitterSchema.TWITTER_COORDS_DEFAULT_TYPE)){
//			return;
//		}
		// figure out how to parse this 
		
	} // end parseGeo

	private void parseUser(JSONObject userJSON){
		ArrayList<String> userKeys = twitterSchema.getTwitterUserKeys();
		for(String uk : userKeys){
			
			String v = "";
			Object o = userJSON.get(uk);
			if(o == null){
				continue;
			} else {
				v = o.toString();
				userProps.put(uk, v);
			}
		}
		
	} // end parseUser
	
	
	
	public String getID(){
		return props.getProperty(twitterSchema.TWITTER_ID_KEY);
	}
	public String getUser(){
		return userProps.getProperty(twitterSchema.TWITTER_USER_ID_KEY);
	}
	public String getUserName(){
		return userProps.getProperty(twitterSchema.TWITTER_USER_NAME_KEY);
	} // end getUniqueID
	
	public long getTimestamp(){
		
		// Wed Aug 27 13:08:45 +0000 2008
		// EEE MMM dd hh:mm:ss Z yyyy
		String createdStr = props.getProperty(twitterSchema.TWITTER_CREATED_TIME_KEY);
		SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy");
		Date d = null;
		try {
			d = sdf.parse(createdStr);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		
		return d.getTime();
	} // end getTimestamp
	
	
	
	public SimpleFeature getSimpleFeature(){
		if(!geoEnabled){
			return null;
		}
		
		SimpleFeatureType sft = twitterSchema.getSimpleFeatureType();
		SimpleFeatureBuilder pointBuilder = new SimpleFeatureBuilder(sft);
		pointBuilder.set("geometry", geo);
		ArrayList<String> keys = twitterSchema.getTwitterKeys();
		//System.out.println("Number of keys = " + keys.size());
		int numString = 0;
		int numDate = 0;
		int numGeo = 0;
		for(int x = 0; x < keys.size(); x++){
			String curK = keys.get(x);
			//System.out.println("current key = " + curK);
//			if(!joGeo.containsKey(curK)){
//				continue;
//			}
			
			String v = "";
			Object o = jels.get(curK);
			if(o == null){
				//System.out.println("\tnull for => " + curK);
				continue;
			} else {
				//System.out.println("1) " + curK + " => " + o.getClass().getCanonicalName() + " => " + o.toString());
				v = o.toString();
			}
			
			String type = twitterSchema.getTwitterClass(curK);
			
			//System.out.println("2) " + curK + " => " + v.getClass().getCanonicalName() + " => " + v.toString() + "\tclass = " + type);

			if(type.equals("Date")){
				
				// format "Sun Mar 15 00:07:22 +0000 2015"
				// EEE MMM dd hh:mm:ss zzzz yyyy 
				//System.out.println("DATE!!!");
				if(v.contains(" ")){
					DateFormat df = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzzz yyyy");
					try {
						Date curDate = df.parse(v);
						pointBuilder.set(curK, curDate);
						numDate++;
					} catch (java.text.ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if(v.length() > 0){
					long t = Long.parseLong(v);
					Date curDate = new Date(t);
					pointBuilder.set(curK, curDate);
					numDate++;
				}
				
			} else if(type.equals("String")){
				//System.out.println("String!!!");

				pointBuilder.set(curK, v);
				numString++;

			}  else if(type.equals("Geometry")){
				if(curK.equals(twitterSchema.TWITTER_COORDS_KEY)){
					pointBuilder.set(curK, geo);
					numGeo++;
				}
			}
			
			
		} // end for
		
		// now add the user information to the feature
		ArrayList<String> ukeys = twitterSchema.getTwitterUserKeys();
		for(int x = 0; x < ukeys.size(); x++){
			String curK = keys.get(x);
			String v = "";
			Object o = jels.get(curK);
			if(o == null){
				//System.out.println("\tnull for => " + curK);
				continue;
			} else {
				//System.out.println("1) " + curK + " => " + o.getClass().getCanonicalName() + " => " + o.toString());
				v = o.toString();
			}
			
			String type = twitterSchema.getTwitterUserClass(curK);
			
			if(type.equals("Date")){
				
				// format "Sun Mar 15 00:07:22 +0000 2015"
				// EEE MMM dd hh:mm:ss zzzz yyyy 
				//System.out.println("DATE!!!");
				if(v.contains(" ")){
					DateFormat df = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzzz yyyy");
					try {
						Date curDate = df.parse(v);
						pointBuilder.set(curK, curDate);
						numDate++;
					} catch (java.text.ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if(v.length() > 0){
					long t = Long.parseLong(v);
					Date curDate = new Date(t);
					pointBuilder.set(curK, curDate);
					numDate++;
				}
				
			} else if(type.equals("String")){
				//System.out.println("String!!!");

				pointBuilder.set(curK, v);
				numString++;

			}
			
			
			
		} // end for
		
		
		
		
		String featureID = "twitter-" + md5;
		System.out.println("geos=" + numGeo + "\t\tdate=" + numDate + "\t\tstring=" + numString);
		return pointBuilder.buildFeature(featureID);
	} // end getSimpleFeatue
	
	
	
	public Geometry getGeomtry(){
		return geo;
	}
	
	
	public boolean hasGeo(){
		return geoEnabled;
	}
	
} // end TwitterGeoItem
