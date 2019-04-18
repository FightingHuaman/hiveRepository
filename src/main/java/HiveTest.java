import org.apache.log4j.Logger;

import java.sql.*;

public class HiveTest {
    public static Logger log = Logger.getLogger(HiveTest.class);
    public static void main(String []args){
        Connected c = new Connected();
        c.connect();
        log.info("this is my message:");
        AppDataCount adc = new AppDataCount();
        adc.web("select prod_name,COUNT(*),month_id,day_id FROM dwa_d_ia_basic_user_app WHERE dwa_d_ia_basic_user_app.prod_name IN (SELECT web_app from web_app) " +
                        "GROUP BY prod_name,month_id,day_id order by month_id,day_id");

        adc.pos("select prod_name,COUNT(*),month_id,day_id FROM dwa_d_ia_basic_user_app WHERE dwa_d_ia_basic_user_app.prod_name IN (SELECT pos_app from pos_app) " +
                        "GROUP BY prod_name,month_id,day_id order by month_id,day_id");


        AppMonthCount amc = new AppMonthCount();
        amc.web("select prod_name,COUNT(*),month_id FROM dwa_m_ia_basic_user_app WHERE dwa_m_ia_basic_user_app.prod_name IN (SELECT web_app from web_app)" +
                "GROUP BY prod_name,month_id order by month_id");

        amc.pos("select prod_name,COUNT(*),month_id FROM dwa_m_ia_basic_user_app WHERE dwa_m_ia_basic_user_app.prod_name IN (SELECT pos_app from pos_app)" +
                "GROUP BY prod_name,month_id order by month_id");

        AppAllCount aac = new AppAllCount();
        aac.allAppData("SELECT COUNT(*),month_id,day_id FROM dwa_d_ia_basic_user_app group by month_id,day_id order by month_id,day_id");
        //SELECT COUNT(distinct device_number,prod_name,prov_id) FROM dwa_d_ia_basic_user_app group by device_number; 1148885,1442685
        aac.allAppMonth("SELECT COUNT(*),month_id FROM dwa_m_ia_basic_user_app group by month_id order by month_id");

        //SELECT COUNT(*) FROM dwa_m_ia_basic_user_app
    }
}

class Connected{
    public String driveName = "org.apache.hive.jdbc.HiveDriver";
    public String url = "jdbc:hive2://10.244.11.226:10000/opdw4_226";
//    public String url = "jdbc:hive2://192.168.2.114:10000/zba_dwa";
    public static Connection conn;
    public void connect(){
        try {
            Class.forName(driveName);
//            conn = DriverManager.getConnection(url,"root","dai123!@#");
            conn = DriverManager.getConnection(url,"opdw4_226","Dfyx#7yRh9");
//            System.out.println("连接成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class AppDataCount extends Connected{
    public static Logger log = Logger.getLogger(AppDataCount.class);
    private PreparedStatement ps1;
    private ResultSet rs1;
    private PreparedStatement ps2;
    private ResultSet rs2;
    String sqlweb = "";
    String sqlpos = "";
    public void web(String sqlweb){
        try {
            long startTime = System.currentTimeMillis();
            ps1 = conn.prepareStatement(sqlweb);
            rs1 = ps1.executeQuery();
            log.info("网贷app(日)：");
            while (rs1.next()){
                log.info(rs1.getString(1)+"   "+rs1.getString(2)+"  " +rs1.getString(3)+"    "+rs1.getString(4));
            }
            long endTime = System.currentTimeMillis();
            log.info("程序运行时间：" + (endTime-startTime) + "ms");
            log.info("\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps1.close();
                rs1.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void pos(String sqlpos){
        try {
            long startTime = System.currentTimeMillis();
            ps2 = conn.prepareStatement(sqlpos);
            rs2 = ps2.executeQuery();
            log.info("pos机app(日)：");
            while (rs2.next()){
                log.info(rs2.getString(1)+"   "+rs2.getString(2)+"  " +rs2.getString(3)+"    "+rs2.getString(4)); //prod_name和count列
            }
            long endTime = System.currentTimeMillis();
            log.info("程序运行时间：" + (endTime-startTime) + "ms");
            log.info("\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps2.close();
                rs2.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

class AppMonthCount extends AppDataCount {
    public static Logger log = Logger.getLogger(AppMonthCount.class);
    private PreparedStatement ps1;
    private ResultSet rs1;
    private PreparedStatement ps2;
    private ResultSet rs2;
    String sqlweb = "";
    String sqlpos = "";
    public void web(String sqlweb){
        try {
            long startTime = System.currentTimeMillis();
            ps1 = conn.prepareStatement(sqlweb);
            rs1 = ps1.executeQuery();
            log.info("网贷app(月)：");
            while (rs1.next()){
                log.info(rs1.getString(1)+"   "+rs1.getString(2)+"  "+rs1.getString(3));
            }
            long endTime = System.currentTimeMillis();
            log.info("程序运行时间：" + (endTime-startTime) + "ms");
            log.info("\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps1.close();
                rs1.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void pos(String sqlpos){
        try {
            long startTime = System.currentTimeMillis();
            ps2 = conn.prepareStatement(sqlpos);
            rs2 = ps2.executeQuery();
            log.info("pos机app(月)：");
            while (rs2.next()){
                log.info(rs2.getString(1)+"   "+rs2.getString(2)+"  "+rs2.getString(3)); //prod_name和count列
            }
            long endTime = System.currentTimeMillis();
            log.info("程序运行时间：" + (endTime-startTime) + "ms");
            log.info("\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                ps2.close();
                rs2.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

class AppAllCount extends Connected{
    public static Logger log = Logger.getLogger(AppAllCount.class);
    private PreparedStatement ps3;
    private PreparedStatement ps4;
    private ResultSet rs3;
    private ResultSet rs4;
    String sqlallappdata = "";
    String sqlallappmonth = "";
    public void allAppData(String sqlallappdata){
        try {
            ps3 = conn.prepareStatement(sqlallappdata);
            rs3 = ps3.executeQuery();
            log.info("日总app数：");
            while (rs3.next()){
                log.info(rs3.getString(1)+"  "+rs3.getString(2)+"   "+rs3.getString(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                rs3.close();
                ps3.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void allAppMonth(String sqlallappMonth){
        try {
            ps4 = conn.prepareStatement(sqlallappMonth);
            rs4 = ps4.executeQuery();
            log.info("月总app数：");
            while (rs4.next()){
                log.info(rs4.getString(1)+" "+rs4.getString(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                rs4.close();
                ps4.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
