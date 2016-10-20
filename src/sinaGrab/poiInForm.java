package sinaGrab;

/**
 * Created by coco1 on 2016/10/20.
 */
public class poiInForm {
    private String lat;
    private String lon;
    private String type;
    private String poiid;
    public poiInForm(String lat, String lon, String type, String poiid) {
        setLat(lat);
        setLon(lon);
        setType(type);
        setPoiid(poiid);
    }
    public String toString() {
        return this.poiid + " " +this.lat + " " + this.lon + " " + this.type;
    }
    public String getPoiid() {
        return poiid;
    }

    public void setPoiid(String poiid) {
        this.poiid = poiid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }
}
