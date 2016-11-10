package sql;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static sql.jdbcConnector.getConn;

/**
 * Created by coco1 on 2016/11/3.
 *
 */
public class TableCreator {
    public static String getSql(int time) {
        return  "CREATE TABLE poiinform\n" + time +
                "(\n" +
                "    lat CHAR(20) NOT NULL,\n" +
                "    lng CHAR(20) NOT NULL,\n" +
                "    poiid CHAR(30) PRIMARY KEY NOT NULL,\n" +
                "    type CHAR(6) NOT NULL,\n" +
                "    checkinnum INT(11) DEFAULT '0'\n" +
                ");";
    }
    public static String getAlert(int time) {
        return  "ALTER TABLE `poistatus_3` ADD UNIQUE ( `poiid` ) ;";
    }
    public static void main(String args[]) throws SQLException {
        Connection conn = getConn();
        PreparedStatement ps;
        for (int i = 1 ; i < 24 ; i ++) {
            String sql =  "ALTER TABLE `poistatus_"+i +"` ADD UNIQUE ( `poiid` ) ;";
            ps = (PreparedStatement) conn.prepareStatement(sql);
            ps.execute();
        }
    }

    private List<String> loadSql(String sqlFile) throws Exception {
        List<String> sqlList = new ArrayList<String>();

        try {
            InputStream sqlFileIn = new FileInputStream(sqlFile);

            StringBuffer sqlSb = new StringBuffer();
            byte[] buff = new byte[1024];
            int byteRead = 0;
            while ((byteRead = sqlFileIn.read(buff)) != -1) {
                sqlSb.append(new String(buff, 0, byteRead));
            }

            // Windows 下换行是 /r/n, Linux 下是 /n
            String[] sqlArr = sqlSb.toString().split("(;//s*//r//n)|(;//s*//n)");
            for (int i = 0; i < sqlArr.length; i++) {
                String sql = sqlArr[i].replaceAll("--.*", "").trim();
                if (!sql.equals("")) {
                    sqlList.add(sql);
                }
            }
            return sqlList;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }
    /**
     * 传入连接来执行 SQL 脚本文件，这样可与其外的数据库操作同处一个事物中
     * @param conn 传入数据库连接
     * @param sqlFile SQL 脚本文件
     * @throws Exception
     */
    public void execute(Connection conn, String sqlFile) throws Exception {
        PreparedStatement stmt = null;
        List<String> sqlList = loadSql(sqlFile);
        stmt = (PreparedStatement) conn.createStatement();
        int i = 0;
        for (String sql : sqlList) {
            stmt.addBatch(sql);
            i ++;
            if (i % 10000 == 0) {
                int[] rows = stmt.executeBatch();
                conn.commit();
                System.out.println("Row count:" + Arrays.toString(rows));
            }
        }
        stmt.executeBatch();
        conn.commit();
    }
}
