package main;

import com.mysql.jdbc.Connection;
import dataRead.CheckIn;
import json.JSONException;
import json.JSONObject;

import java.io.File;
import java.io.IOException;
import com.mysql.jdbc.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static dataRead.Util.*;
import static sql.jdbcConnector.checkInInsert;
import static sql.jdbcConnector.getConn;
import static sql.jdbcConnector.insertSql;

/**
 * Created by coco1 on 2016/10/19.
 */
public class firstPartMain {
    public static String[] cityname = {"沧州市","承德市", "邯郸市", "衡水市", "廊坊市","石家庄市", "唐山市", "天津市", "邢台市", "张家口市"};
    public static void main(String args[]) throws IOException, JSONException, SQLException {
        Connection connection = getConn();
        connection.setAutoCommit(false);
        PreparedStatement ps = (PreparedStatement) connection.prepareStatement(insertSql);
        List<CheckIn> temp;
        List<File> li = new LinkedList<>();
        for (String s  : cityname) {
            li.addAll(getFilePath(new File("F:\\OneDrive\\文档\\河北省微博数据\\" + s + File.separator)));
        }
        for (File f : li) {
            temp = new LinkedList<>();
            System.out.println("开始读取:" + f.toString());
            List<String> tempjson = dataRead(f);
            for (String s : tempjson) {
                JSONObject json = null;
                try {
                    json = new JSONObject(s);
                }catch (JSONException e) {
                    e.printStackTrace();
                    System.out.println(s);
                    continue;
                }
                CheckIn c = readJson(json);
                if (c != null) {
                    temp.add(c);
                }
            }
            checkInInsert(temp, connection, ps);
            System.out.println("完成" + f.getName());
        }
    }
}
