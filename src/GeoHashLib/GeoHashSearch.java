package GeoHashLib;

/**
 * Created by coco1 on 2016/11/25.
 *
 * 纬度-90~90 lat
 * 经度-180~180 lng
 */
public class GeoHashSearch {
    public static String searchByR(double r) {
        double level;
        double R = 2 * r;
        int l = (int)(Math.log(360 / R)) * 2 - 1;
        double len = 180 / (Math.pow(2, (l * 2 - 1)));
        if (len >= R) level = l / 2;
        else level = l;
        return level + "";
    }
    public static void main(String args[]) {
        System.out.println(searchByR(0.1)); //
    }
}
