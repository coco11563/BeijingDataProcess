package MRCompetion.ThreadClass;

/**
 * Created by coco1 on 2016/11/3.
 */
public class PoiStatusUpdateThread {
    public static String generateSql(String field, int time) {
        return "update rawdata.poistatus_"+time + " with(rowlock) set checkinnum=checkinnum+1 where poiid=\'"+field + "\'";
    }
}
