package MRCompetion.ThreadClass;

/**
 * Created by coco1 on 2016/11/3.
 */
public class PoiStatusCheckThread {
    public static String generateSql(String poiid) {
        return "select count(*) from rawdata.poiinform a WHERE a.poiid=\'" + poiid + "\'";
    }
}
