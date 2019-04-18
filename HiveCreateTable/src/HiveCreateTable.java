import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class HiveCreateTable {
//    先导入驱动
    private static String driveName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws Exception {
//     与数据库连接
        Class.forName(driveName);
        Connection conn = DriverManager.getConnection("jdbc:hive://localhost:10000/userdb","root","123456");
        Statement stmt = conn.createStatement();
        stmt.executeQuery("create table IF NOT EXISTS"
        +"employee(eid int ,name String ,salary String ,destignation String)"
        +"COMMENT 'Employee details'"
        +"ROW FORMAT DELIMITED"
        +"FIELDS TERMINATED BY '\t'"
        +"LINES TERMINATED BY '\n'"
        +"STORED AS TEXTFILE;");

        System.out.println("Table employee created.");
        conn.close();
    }
}
