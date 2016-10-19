package dataRead;

import json.JSONArray;
import json.JSONException;
import json.JSONObject;

import java.io.*;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by coco1 on 2016/10/17.
 *
 * 用来实现 文件 -> json -> check in
 */
public class Util {
    static List<File>  filepath;
    private static final String outputpath = "C:\\Users\\coco1\\IdeaProjects\\BeijingDataProcess\\output";
    public Util(File dire) {
        getFilePath(dire);
    }
    /**
     * 从jsonObject中读取出checkin讯息
     *
     * @param jsonObject jsonObj
     *
     * @return checkin
     */
    public static CheckIn readJson(JSONObject jsonObject)  {
        try {
            JSONArray coor = (JSONArray) jsonObject.getJSONObject("geo").get("coordinates");
            String content = (String)jsonObject.get("text");
            int time = getTime((String)jsonObject.get("created_at"));
            String poiid  = jsonObject.getJSONArray("annotations").getJSONObject(0).getJSONObject("place").getString("poiid");
            return new CheckIn(content, Double.parseDouble(coor.get(0).toString()), Double.parseDouble(coor.get(1).toString()), poiid, time);
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
    public static void getFilePath ( File file) {
        filepath = new LinkedList<>();
        if(file.isDirectory())
        {
            File f[]= file.listFiles();
            if(f!=null)
            {
                for(int i=0;i<f.length;i++)
                {
                    getFilePath(f[i]);
                }
            }
        } else {
                System.out.println(file.toString());
                filepath.add(file);
        }
    }

    /**
     * 获取json讯息
     *
     * @param file 文件地址
     *
     * @return jsonOBJ
     */
    private static List<String> dataRead(File file) throws IOException {
        InputStreamReader fReader = new InputStreamReader(new FileInputStream(file),"UTF-8");
        BufferedReader reader = new BufferedReader(fReader) ;
        List<String> ret = new LinkedList<>();
        String temp;
        while ((temp = reader.readLine()) != null) {
            ret.add(temp);
        }
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
    public static void main(String args[]) throws IOException, JSONException {
        List<String> json = dataRead(new File("C:\\Users\\coco1\\IdeaProjects\\BeijingDataProcess\\data\\2014-09-19-鍖椾含甯_json"));
        System.out.println(json.get(1));
        CheckIn c = readJson(new JSONObject(json.get(1)));
        System.out.print(c);
    }
}
