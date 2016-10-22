package sinaGrab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import json.JSONArray;
import json.JSONException;
import json.JSONObject;

public class getCityName {
	/*
	 * 百度API所需要的key
	 */
	static String [] keystore = {
						  "N3kkevWBh1hTSuigNHODGmYiUsngR5EM"};
	/**
	 * 用来调用url
	 * @param url
	 * @return String from web
	 */
	private static String pcn_connUrl(String url){
		URL getUrl;
		BufferedReader br = null;
		StringBuffer buffer = new StringBuffer() ;
		try 
		{
			getUrl = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) getUrl.openConnection();
			connection.setConnectTimeout(30*1000);
			connection.connect();
			InputStreamReader isr = new InputStreamReader(connection.getInputStream(),"utf-8");
			br = new BufferedReader(isr); 
			int s;
			while((s = br.read())!=-1)
			{
				buffer.append((char)s);
			}
		}
		catch (IOException e) 
		{
			System.out.println ("error" );
		}
		return  buffer.toString() ;	
	}
	/**
	 * 从json中解析出坐标
	 * 然后根据这个坐标调用百度API返回城市名和省市名
	 * @param jsonObject
	 * @return String[] 省市名
	 * @throws IOException
	 */
	private static String[] pcn_getProCityNameURL(JSONObject jsonObject) throws  IOException
	{
		String[]			cityName			=		null;
		try
		{
			double 				location[] 			= 		pcn_getCoordinates( jsonObject ) ;	
//			String 				url						=		"http://api.map.baidu.com/geocoder?location="+location[0]+","+location[1]+"&output=json&key=28bcdd84fae25699606ffad27f8da77b" ;
			String  			url						=  		"http://api.map.baidu.com/geocoder/v2/?ak=N3kkevWBh1hTSuigNHODGmYiUsngR5EM&location="+location[0]+","+location[1]+"&output=json&coordtype=gcj02ll";
			String 				baiduLocation 	= 		pcn_connUrl(url) ;
			JSONObject 		temp_object		=		new JSONObject(baiduLocation) ;
			temp_object		= 		temp_object.getJSONObject("result") ;
			temp_object		= 		temp_object.getJSONObject("addressComponent") ;
			cityName			=		new String[2] ;
			cityName[0]		=		temp_object.getString("province") ;
			cityName[1]		=		temp_object.getString("city") ;
			
			if(cityName[1].contains ( "直辖县级行政单位" ) )
				cityName[1]	=		temp_object.getString("district") ;
			
			System.out.println ( location[0]+"\t"+location[1] + ":\t" + cityName[0]+"-"+cityName[1]);
			return cityName;
		}
		catch(Exception e)
		{
			cityName		=		new String[2] ;
			cityName[0]	=	"Exception" ;
			cityName[1]	=	"Exception" ;
			return cityName;
		}
	}
	/**
	 * 输入坐标，返回省市名
	 * @return 省市名 0province 1city
	 * @throws IOException
	 */
	public static String[] pcn_getProCityNameURL(double [] location ) throws  IOException
	{
		
		String[]			cityName			=		null;
		String 				key 				= 		"N3kkevWBh1hTSuigNHODGmYiUsngR5EM";
		try
		{
//			String 				url						=		"http://api.map.baidu.com/geocoder?location="+location[0]+","+location[1]+"&output=json&key=28bcdd84fae25699606ffad27f8da77b" ;
			String  			url						=  		"http://api.map.baidu.com/geocoder/v2/?ak="+key+"&location="+location[0]+","+location[1]+"&output=json&coordtype=gcj02ll";
			String 				baiduLocation 	= 		pcn_connUrl(url) ;
			JSONObject 		temp_object		=		new JSONObject(baiduLocation) ;
			temp_object		= 		temp_object.getJSONObject("result") ;
			temp_object		= 		temp_object.getJSONObject("addressComponent") ;
			cityName			=		new String[2] ;
			cityName[0]		=		temp_object.getString("province") ;
			cityName[1]		=		temp_object.getString("city") ;
			
			if(cityName[1].contains ( "直辖县级行政单位" ) )
				cityName[1]	=		temp_object.getString("district") ;
			
//			System.out.println ( location[0]+"\t"+location[1] + ":\t" + cityName[0]+"-"+cityName[1]);
			return cityName;
		}
		catch(Exception e)
		{
			System.out.println(e.toString());
			cityName			=		new String[2] ;
			cityName[0] = "exception province";
			cityName[1] = "exception city";
			return cityName;
		}
	}
	/**
	 * 负责从输入的jsonobj中解析出地理坐标
	 * @param json_object
	 * @return 经纬度坐标
	 * @throws JSONException
	 */
	private static double[] pcn_getCoordinates(JSONObject json_object) throws JSONException
	{
		JSONObject 	geo_object 		= 		json_object.getJSONObject ( "geo" ) ;
		JSONArray		coor_jsarray		=		geo_object.getJSONArray ( "coordinates" ) ;
		double 			location[] 			= 		new double[2] ;
		location[0]		=		coor_jsarray.getDouble ( 0 ) ;
		location[1]		=		coor_jsarray.getDouble ( 1 ) ;
		return location ;
	}

	/**
	 * 测试该坐标点是否为北京坐标
	 * @param lat 纬度
	 * @param lng 经度
	 * @return 是或者不是
	 */
	public static boolean isBeijing(double lat, double lng){
		double[] 			location 			= 		new double[2];
		location[0] = lat;
		location[1] = lng;
        String cityName = null;
        try {
            cityName = pcn_getProCityNameURL(location)[1];
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("key也许出问题了");
            return false;
        }
        System.out.println("是" + cityName);
        return cityName.equals("北京市");
	}

    /**
     *
     * @param o 传进来这个点的json讯息
     * @param lat 传进来这个点的lat
     * @param lon 传进来这个点的lon
     * @return 是或者不是
     */
    public static boolean isBeijing(JSONObject o, double lat, double lon){
        String cityName = null;
        try {
            cityName = o.getJSONObject("district_info").getString("province");
        } catch (JSONException e) {
            e.printStackTrace();
            return isBeijing(lat, lon);
        }
        System.out.println(cityName);
        return cityName.equals("北京市");
    }
	
	public  static void main(String args[]) throws IOException
	{
		String[]			cityName			=		null;
		double[] 			location 			= 		new double[2];
		location[0] = 39.633;
		location[1] = 116.479316;
		cityName = pcn_getProCityNameURL(location) ;
		System.out.println(cityName[1]);
		System.out.println(isBeijing(location[0],location[1]));
	}
}
