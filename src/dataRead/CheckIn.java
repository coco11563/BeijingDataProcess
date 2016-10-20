package dataRead;

import static dataRead.Util.removeFourChar;

/**
 * Created by coco1 on 2016/10/15.
 *
 * 解析的json文件中所需要的用于POI落点分析的数据
 *
 * 包含：
 *
 * 1.时间（只需要整点信息）
 * 2.地点（lat与long）
 * 3.行为（poiid）
 * 4.内容（*）
 */
public class CheckIn {
    private String content;
    private String lat;
    private String lng;
    private String poiid;
    private int time;
    private String idstr;
    private String date;
    /**
     *
     * @param con 内容
     * @param lat 经度
     * @param lng 纬度
     * @param poiid 点id
     * @param date 时间
     */
    public CheckIn(String idstr, String con, String lat, String lng, String poiid, String date, int time) {
        this.idstr = idstr;
        content = removeFourChar(con);
        this.lat = lat;
        this.lng = lng;
        this.poiid = poiid;
        this.date = date;
        this.time = time;
    }
    public String toString() {
        return idstr + " " + poiid + " " + lat + " " + lng  + " " + date + " " +time +" "+ content;
    }
    public static void main(String args[]) {
    }
    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLng() {
        return lng;
    }

    public void setLng(String lng) {
        this.lng = lng;
    }

    public String getPoiid() {
        return poiid;
    }

    public void setPoiid(String poiid) {
        this.poiid = poiid;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getIdstr() {
        return idstr;
    }

    public void setIdstr(String idstr) {
        this.idstr = idstr;
    }

    public int getTime() {
        return time;
    }

    public void setT(int Time) {
        this.time = Time;
    }

}


