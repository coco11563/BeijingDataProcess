package dataRead;

import json.JSONArray;
import json.JSONException;
import json.JSONObject;

import java.io.*;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import static sql.jdbcConnector.checkInInsert;

/**
 * Created by coco1 on 2016/10/17.
 *
 * 用来实现 文件 -> json -> check in
 */
public class Util {
    private static final String outputpath = "C:\\Users\\coco1\\IdeaProjects\\BeijingDataProcess\\output";
    /**
     * 从jsonObject中读取出checkin讯息
     *
     * @param jsonObject jsonObj
     *
     * @return checkin
     */
    public static CheckIn readJson(JSONObject jsonObject)  {
        try {
            String idstr = jsonObject.getString("idstr");
            JSONArray coor = (JSONArray) jsonObject.getJSONObject("geo").get("coordinates");
            String content = (String)jsonObject.get("text");
            String date = jsonObject.getString("created_at");
            int time = getTime((String)jsonObject.get("created_at"));
            String poiid  = jsonObject.getJSONArray("annotations").getJSONObject(0).getJSONObject("place").getString("poiid");
            return new CheckIn(idstr,content, coor.get(0).toString(), coor.get(1).toString(), poiid, date,time);
        } catch (JSONException e ) {
            e.printStackTrace();
            return null;
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * 2016/10/11 17:36 测试通过
     *
     * 在输入的File类型地址下获取所有文件名
     *
     * 获取文件原地址
     *
     * @param file 文件地址
     */
    public static List<File> getFilePath ( File file) {
        List<File> filepath = new LinkedList<>();
        if(file.isDirectory())
        {
            File f[]= file.listFiles();
            if(f!=null)
            {
                for(int i=0;i<f.length;i++)
                {
                    filepath.addAll(getFilePath(f[i]));
                }
            }
        } else {
                System.out.println(file.toString());
                filepath.add(file);
        }
        return filepath;
    }

    /**
     * 获取json讯息
     *
     * @param file 文件地址
     *
     * @return jsonOBJ
     */
    public static List<String> dataRead(File file) throws IOException {
        InputStreamReader fReader = new InputStreamReader(new FileInputStream(file),"UTF-8");
        BufferedReader reader = new BufferedReader(fReader) ;
        List<String> ret = new LinkedList<>();
        String temp;
        while ((temp = reader.readLine()) != null) {
            ret.add(temp);
        }
        ret.remove(ret.size() - 1);
        reader.close();
        fReader.close();
        return ret;
    }

    /**
     * 解析json时使用的类
     *
     * @param time 时间String
     *
     * @return int时间
     */
    @SuppressWarnings("deprecation")
    public static int getTime(String time){
        Date d = new Date(time);
        return d.getHours();
    }
    public static void outputData(List<CheckIn> li) throws IOException {
        File output = new File(outputpath + "//rawData.txt");
        FileWriter 	write_fw = new FileWriter(output, true);
        if (!output.isFile()) output.mkdirs();
        for (CheckIn c : li) {
            write_fw.write(c.toString());
        }
        write_fw.close();
    }

    /**
     * 替换四个字节的字符 '\xF0\x9F\x98\x84\xF0\x9F）的解决方案 😁
     * @author ChenGuiYong
     * @data 2015年8月11日 上午10:31:50
     * @param content 输入字符
     * @return 返回处理完毕的字符
     */
    public static String removeFourChar(String content) {
        byte[] conbyte = content.getBytes();
        for (int i = 0; i < conbyte.length; i++) {
            if ((conbyte[i] & 0xF8) == 0xF0) {
                for (int j = 0; j < 4; j++) {
                    conbyte[i+j]=0x30;
                }
                i += 3;
            }
        }
        content = new String(conbyte);
        return content.replaceAll("0000", "");
    }
    public static void main(String args[]) throws IOException, JSONException, SQLException {
        List<String> json = dataRead(new File("C:\\Users\\coco1\\IdeaProjects\\BeijingDataProcess\\data\\2016-02-17-鍖椾含甯_json"));
        System.out.println(json.get(1));
        CheckIn c = readJson(new JSONObject(json.get(0)));
        System.out.print(c);
    }
}
