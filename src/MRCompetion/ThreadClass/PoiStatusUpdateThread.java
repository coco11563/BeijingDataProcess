package MRCompetion.ThreadClass;

/**
 * Created by coco1 on 2016/11/3.
 */
public class PoiStatusUpdateThread {
    public static String generateSql(String field) {
        return "update rawdata.poiinform with(rowlock) set checkinnum=checkinnum+1 where poiid=\'"+field + "\'";
    }
}
