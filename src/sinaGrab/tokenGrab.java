package sinaGrab;

import json.JSONArray;
import json.JSONException;
import json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by coco1 on 2016/10/21.
 */
public class tokenGrab {
    public static List<String> token = new ArrayList<>();

    public tokenGrab(String file) throws IOException, JSONException {
        read_date_config(file);
    }
    /**
     * 读取时间配置文件的函数。
     *
     * @return
     *
     * @throws JSONException
     *
     * @throws IOException
     *
     */
    public static void read_date_config(String file) throws JSONException, IOException {
        System.out.println(file);
        String str_date_config_json = readFile(file);
        JSONObject date_config_json = new JSONObject(str_date_config_json);
        JSONArray date_json = date_config_json.getJSONArray("access_token");
        int i = 0 ;
        while (i < date_json.length()) {
            token.add(date_json.getString(i));
            i++;
        }
    }


    /**
     *
     * @param filePath
     * @return
     */
    private static String readFile(String filePath) {
        String data = "";
        try {
            String encoding = "UTF-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists()) { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    data += lineTxt;
                }
                read.close();
            } else {
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
        return data;
    }
}
