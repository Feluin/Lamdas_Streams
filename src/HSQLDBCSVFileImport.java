import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HSQLDBCSVFileImport {
    private Connection connection;
    private String driverName = "jdbc:hsqldb:";
    private String username = "sa";
    private String password = "";

    public static void main(String... args) {
        // HSQLDBCSVFileImport hsqldbcsvFileImport = new HSQLDBCSVFileImport();
        // hsqldbcsvFileImport.init();
        // hsqldbcsvFileImport.importCSVFile(Configuration.instance.dataPath + "records.csv");
        // hsqldbcsvFileImport.shutdown();
    }

    public void startup() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            String databaseURL = driverName + Configuration.instance.dataPath + "records.db";
            connection = DriverManager.getConnection(databaseURL, username, password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public synchronized void update(String sqlStatement) {
        try {
            Statement statement = connection.createStatement();
            int result = statement.executeUpdate(sqlStatement);
            if (result == -1)
                System.out.println("error executing " + sqlStatement);
            statement.close();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }
    }

    public void dropTable() {
        System.out.println("--- dropTable");

        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("DROP TABLE data");
        System.out.println("sqlStringBuilder : " + sqlStringBuilder.toString());

        update(sqlStringBuilder.toString());
    }

    public void createTable() {
        StringBuilder sqlStringBuilder = new StringBuilder();
        sqlStringBuilder.append("CREATE TABLE data ").append(" ( ");
        sqlStringBuilder.append("id BIGINT NOT NULL").append(",");
        sqlStringBuilder.append("size VARCHAR(1) NOT NULL").append(",");
        sqlStringBuilder.append("weight INTEGER NOT NULL").append(",");
        sqlStringBuilder.append("source VARCHAR(1) NOT NULL").append(",");
        sqlStringBuilder.append("destination VARCHAR(1) NOT NULL").append(",");
        sqlStringBuilder.append("isExpress VARCHAR(5) NOT NULL").append(",");
        sqlStringBuilder.append("isValue VARCHAR(5) NOT NULL").append(",");
        sqlStringBuilder.append("isFragile VARCHAR(5) NOT NULL").append(",");
        sqlStringBuilder.append("PRIMARY KEY (id)");
        sqlStringBuilder.append(" )");
        update(sqlStringBuilder.toString());
    }

    public void init() {
        startup();
        dropTable();
        createTable();
    }

    public String buildSQLStatement(long id, String size, double weight, String source, String destination,
                                    String isExpress, String isValue, String isFragile) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("INSERT INTO data (id,size,weight,source,destination,isExpress,isValue,isFragile) VALUES (");
        stringBuilder.append(id).append(",");
        stringBuilder.append("'").append(size).append("'").append(",");
        stringBuilder.append(weight).append(",");
        stringBuilder.append("'").append(source).append("'").append(",");
        stringBuilder.append("'").append(destination).append("'").append(",");
        stringBuilder.append("'").append(isExpress).append("'").append(",");
        stringBuilder.append("'").append(isValue).append("'").append(",");
        stringBuilder.append("'").append(isFragile).append("'");
        stringBuilder.append(")");
        //System.out.println(stringBuilder.toString());
        return stringBuilder.toString();
    }

    public void insert(long id, String size, double weight, String source, String destination,
                       String isExpress, String isValue, String isFragile) {
        update(buildSQLStatement(id, size, weight, source, destination, isExpress, isValue, isFragile));
    }

    public void importCSVFile(String fileName) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strings = line.split(";");
                insert(Integer.parseInt(strings[0]), strings[1], Double.parseDouble(strings[2]),
                        strings[3], strings[4], strings[5], strings[6], strings[7]);
            }
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }

    public void shutdown() {
        try {
            Statement statement = connection.createStatement();
            statement.execute("SHUTDOWN");
            connection.close();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }
    }
}