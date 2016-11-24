package GeoHashLib.Hbase;

import java.util.List;

/**
 * Created by Administrator on 2016/11/24.
 */
class Split {
    int end;
    public int start;
    Split(int startrow, int endrow) {
        this.end = endrow;
        this.start = startrow;
    }
}
