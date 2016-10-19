package dataRead;

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
    private double lat;
    private double lng;
    private String poiid;
    private int date;
    public boolean isNull() {
        return content == null || lat == 0 || lng == 0 || poiid == null || date == 0;
    }
    /**
     *
     * @param con 内容
     * @param lat 经度
     * @param lng 纬度
     * @param poiid 点id
     * @param date 时间
     */
    public CheckIn(String con, double lat, double lng, String poiid, int date) {
        content = con;
        this.lat = lat;
        this.lng = lng;
        this.poiid = poiid;
        this.date = date;
    }
    public String toString() {
        return poiid + " " + lat + " " + lng  + " " + date + " " + content;
    }
    public static void main(String args[]) {
        CheckIn c = new CheckIn("12", 12.1, 222.1, "123D" , 1);
        System.out.print(c.toString());
    }
}


