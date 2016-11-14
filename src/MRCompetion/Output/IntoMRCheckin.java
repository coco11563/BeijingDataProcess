package MRCompetion.Output;

import dataRead.CheckIn;

/**
 * Created by coco1 on 2016/11/14.
 */
public class IntoMRCheckin {
    private String lat;
    private String lon;
    private String type;
    private String poiid;
    private int datetime;
    public IntoMRCheckin(CheckIn checkIn, String type) {
        lat = checkIn.getLat();
        lon = checkIn.getLng();
        this.type = type;
        poiid = checkIn.getPoiid();
        datetime = checkIn.getTime();
    }

    public String toString() {
        return lat + " " + lon + " " + type + " " + poiid + " " + datetime;
    }
}
