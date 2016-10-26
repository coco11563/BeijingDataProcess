package main;


import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import json.JSONException;
import sinaGrab.poiInForm;
import sinaGrab.tokenGrab;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static sinaGrab.dataGrab.processIgnoreData;
import static sinaGrab.tokenGrab.token;
import static sql.jdbcConnector.*;

/**
 * 用以完成POI的补全
 * Created by coco1 on 2016/10/22.
 */
public class ThirdPartMain {
    public static void main(String args[]) throws SQLException, IOException, JSONException, InterruptedException {
        tokenGrab tg = new tokenGrab("C:\\Users\\coco1\\IdeaProjects\\BeijingDataProcess\\Token\\key.json");
        Connection connection = getConn();
        connection.setAutoCommit(false);
        List<String> getpoi = getAllPoiid();
        List<String> ignorePoi = new ArrayList<>();
        for (String s : getpoi) {
            if (!have(s,connection)) {
                ignorePoi.add(s);
            }
        }
        List<poiInForm> ignorePoiData = processIgnoreData(ignorePoi, token);
        System.out.println("====即将插入" + ignorePoiData.size() + "条数据====");
        PreparedStatement ps = (PreparedStatement) connection.prepareStatement(insertPoiInform);
        poiInformInsert(ignorePoiData, connection, ps);
        System.out.println("====插入完成" + ignorePoiData.size() + "条数据====");
        ps.close();
        connection.close();
    }
}
