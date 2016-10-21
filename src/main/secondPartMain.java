package main;

import json.JSONException;
import sinaGrab.tokenGrab;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import static sinaGrab.dataGrab.processData;
import static sinaGrab.dataGrab.returnposition;
import static sinaGrab.tokenGrab.token;

/**
 * Created by coco1 on 2016/10/21.
 */
public class secondPartMain {
    public static void main(String ars[]) throws IOException, JSONException, SQLException, InterruptedException {
        //声明抓取区域
        ArrayList<String[]> grabPosition = returnposition();
        //声明token
        tokenGrab tg = new tokenGrab("C:\\Users\\coco1\\IdeaProjects\\BeijingDataProcess\\Token\\key.json");
        //开始抓取
        processData(grabPosition,token);
    }
}
