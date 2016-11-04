package MRCompetion.ThreadClass.Deprecated;

/**
 * Created by coco1 on 2016/11/3.
 */
@Deprecated
public class PoiStatusCheckThread {
    public static String generateSql(String poiid) {
        return "select count(*) from rawdata.poiinform a WHERE a.poiid=\'" + poiid + "\'";
    }
}
