package sinaGrab;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import json.JSONArray;
import json.JSONException;
import json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.*;

import static sinaGrab.getCityName.isBeijing;
import static sql.jdbcConnector.*;

/**
 * Created by coco1 on 2016/10/20.
 */
public class dataGrab {
    private static final String poi_show_url = "https://api.weibo.com/2/place/pois/show.json?";
    private static final String nearby_poi_url = "https://api.weibo.com/2/place/nearby/pois.json?";

    /**
     * @param token    access token
     * @param poiid    兴趣点ID
     * @param base_app 是否只获取当前应用的数据。0为否（所有数据），1为是（仅当前应用），默认为0。
     * @return
     */

    public static String genaratePoiShow(String token, String poiid, int base_app) {
        return poi_show_url + "access_token=" + token + "&poiid=" + poiid + "&base_app=" + base_app;
    }

    /**
     * @param token access token
     * @param lat   经度
     * @param lon   纬度
     * @param range 抓取范围
     * @param count 单页数据量   20 -50
     * @param page  页码
     * @return url
     */
    public static String genarateNearbyPoi(String token, String lat, String lon, String range, String count, String page) {
        return nearby_poi_url + "access_token=" + token + "&lat=" + lat + "&long=" + lon + "&range=" + range + "&count=" + count + "&page=" + page;
    }

    /**
     * @param url String型的URL地址
     * @return json文件
     */
    public static String connUrl(String url) {
        URL getUrl;
        try {
            getUrl = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) getUrl
                    .openConnection();
            connection.setReadTimeout(20 * 1000);
            connection.connect();
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    connection.getInputStream(), "utf-8"));// 设置
            String jason_data = "";
            while ((jason_data = reader.readLine()) != null) {
                return jason_data;
            }
        } catch (IOException e) {
            return "error";
        }
        return "error";
    }

    public static void processData(List<String[]> coor, List<String> token) throws InterruptedException, SQLException {
        Connection connection = getConn();
        connection.setAutoCommit(false);
        PreparedStatement ps = (PreparedStatement) connection.prepareStatement(insertPoiInform);
        int access_token_total = token.size();
        int access_token_current = 0;            // 当前钥匙编码
        int count = 50;            // 每页数据量
        int data_total_number = 0;            // 记录地区总的微博量
        String range = "5566";
        int error_back = 0;            // 一次URL取数据累计取空次数
        for (String[] aCoor : coor) {
            String lat = aCoor[0];
            String lon = aCoor[1];
            int pages = 1;
            label:
            for (int page = 1; page <= pages; page++) {
                // 计算使用的账户
                access_token_current = access_token_current % (access_token_total);
                String URL = genarateNearbyPoi(token.get(access_token_current), lat, lon, range, Integer.toString(count), Integer.toString(page));
                System.out.println(URL);
                Thread.sleep(100);//降低访问频率的关键点一：休眠
                System.out.println("纬度：" + lat + "度。");
                System.out.println("经度：" + lon + "度。");
                String json_data = connUrl(URL);
                //目前取消第二次抓取
                //空抓后有数据/空抓总数：71.0/63786。
                //优化后可达0/9k
                switch (json_data) {
                    case "[]":
                        break label;

                    // 情况二：取错：ERROR，说明该账户请求次数超出限制。
                    case "error":
                        error_back++;
                        page--;
                        access_token_current++;
                        if (error_back < access_token_total * 2)//如果刷过两轮所有的access_token都取错，证明次数不够了，休眠到下一个整点才开始继续取数据。
                        {
                            System.out.println("-----------取错->换Key-------------\r\n\r\n");
                        } else {
                            long time = getSecondsToNextClockSharp();
                            System.out.println("---------取错->休眠至整点:" + time + "s----------\r\n\r\n");
                            Thread.sleep(1000 * time);
                            error_back = 0;
                        }
                        break;
                    // 情况三：正常
                    default:
                        // 解析json数据
                        try {
                            JSONObject js = new JSONObject(json_data);
                            if (!js.getString("total_number").equals("")) {
                                data_total_number = Integer.parseInt(js.getString("total_number"));
                            } else {
                                break;
                            }
                            //下一次页面处理计算
                            pages = data_total_number / 50 + 1;
                            // 建立数据数组
                            JSONArray array = new JSONArray();
                            if (!js.isNull("pois"))
                                array = js.getJSONArray("pois");
                            else
                                break;
                            List<poiInForm> li = new LinkedList<>();
                            // 插入到数据库中数据
                            for (int j1 = 0; j1 < array.length(); j1++) {
                                poiInForm temp = getInform((JSONObject) array.get(j1));
                                if (temp != null)
                                    li.add(temp);
                                else
                                    System.err.println("wrong place");
                            }
                            if (li.size() > 0) {
                                poiInformInsert(li, connection, ps);
                                System.out.println("<-----------入库------------->");
                            }
                            access_token_current++;//降低访问频率的关键点一：提高access_token切换次数。
                        } catch (JSONException e) {
                            System.out.println(e.getMessage());
                        }
                        // 获取总数据
                        break;
                }
            }
        }
    }

    /**
     * 解析抓取的poi数据
     * @param o 抓取到的JSONObject
     * @return poiInform
     */
    private static poiInForm getInform(JSONObject o) {
        try {
            String poiid = o.getString("poiid");
            String lon = o.getString("lon");
            String lat = o.getString("lat");
            String type = o.getString("category");
            if (isBeijing(o, Double.parseDouble(lat),Double.parseDouble(lon))) {//是北京
                return new poiInForm(lat, lon, type, poiid);
            } else {
                return null;
            }
        } catch (JSONException e) {
            System.out.println(o.toString());
            return null;
        }
    }
    private static poiInForm getIgnoreInform(JSONObject o) {
        try {
            String poiid = o.getString("poiid");
            String lon = o.getString("lon");
            String lat = o.getString("lat");
            String type = o.getString("category");
            return new poiInForm(lat, lon, type, poiid);
        } catch (JSONException e) {
            System.out.println(o.toString());
            return null;
        }
    }
    /**
     * 获取到下一个时刻所需要的秒数。
     * @return
     */
    public static long getSecondsToNextClockSharp() {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.HOUR, 1);
        //c.set(Calendar.HOUR, 1);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        return (c.getTime().getTime() - System.currentTimeMillis()) / 1000;
    }
    public static ArrayList<String[]> returnposition()
    {
        ArrayList<String[]> ret = new ArrayList<>();
        LinkedList < AreaData > location = new LinkedList < AreaData > ( );
        location.add ( new AreaData ( 39.42358,40.793798,115.479316,117.282251 ) );// 京
        double lat_min,lat_max,lon_min,lon_max ;
        for (AreaData lat_lon : location) {
            lat_min = lat_lon.getLat_min();
            lon_min = lat_lon.getLon_min();
            lat_max = lat_lon.getLat_max();
            lon_max = lat_lon.getLon_max();
            double latlon = 0.05;

            for (int i = 0; i < 2; i++) {
                if (i == 1) {
                    lat_min = lat_min + latlon;
                    lon_min = lon_min + latlon;
                    lat_max = lat_max - latlon;
                    lon_max = lon_max - latlon;
                }
                for (double lat = lat_min; lat < lat_max - latlon; lat = lat + 2 * latlon) {
                    for (double lon = lon_min; lon < lon_max - latlon; lon = lon + 2 * latlon) {
                        //System.out.println(lat+ " " +lon);
                        String[] coordinary = new String[2];
                        coordinary[0] = Double.toString(lat);
                        coordinary[1] = Double.toString(lon);
                        ret.add(coordinary);
                    }
                }

            }
        }
        return ret;
    }

    public static List<poiInForm> processIgnoreData(List<String> poi, List<String> token) throws InterruptedException, SQLException, JSONException {
        Connection connection = getConn();
        connection.setAutoCommit(false);
        int grab_total_num = 0;
        int not_exist_num = 0;
        int access_token_total = token.size();
        int access_token_current = 0;            // 当前钥匙编码
        int error_back = 0;
        PreparedStatement ps = (PreparedStatement) connection.prepareStatement(insertPoiInform);
        List<poiInForm> li = new LinkedList<>();
        for (String s : poi) {
            grab_total_num++;
            access_token_current = access_token_current % (access_token_total);
            String url = genaratePoiShow(token.get(access_token_current),s,0);
            System.out.println(url);
            String json_data = connUrl(url);
            if (json_data.equals("[]")) {
                // 情况二：取错：ERROR，说明该账户请求次数超出限制。
            } else if (json_data.equals("error")) {
                error_back++;
                access_token_current++;
                if (error_back < access_token_total * 2)//如果刷过两轮所有的access_token都取错，证明次数不够了，休眠到下一个整点才开始继续取数据。
                {
                    System.out.println("-----------取错->换Key-------------\r\n\r\n");
                }

                // 情况三：正常
            } else {// 解析json数据
                try {
                    JSONObject js = new JSONObject(json_data);
                    if (js.has("error")||js.getString("error").equals("Target poi does not exist!")) {
                        System.err.println("this poi point dose not exist, so delete it");
                        System.err.print(not_exist_num + " / " + grab_total_num);
                        not_exist_num++;
//                        delCheckin(s,connection);
                        continue;
                    }
                    // 插入到数据库中数据
                    poiInForm temp = getInform(js);
                    if (temp != null)
                        li.add(temp);
                    else
                        System.err.println("wrong");
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                access_token_current++;//降低访问频率的关键点一：提高access_token切换次数。

            }
        }
        return li;
    }
}
