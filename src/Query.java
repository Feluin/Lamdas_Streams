import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class Query implements IQuery {
    private Connection connection;
    private String driverName = "jdbc:hsqldb:";
    private String username = "sa";
    private String password = "";
    private BufferedWriter bufferedWriter;

    public static void main(String... args) {
        Query query = new Query();
        query.startup();

        query.executeSQL01();
        query.executeSQL02();
        query.executeSQL03();
        query.executeSQL04();
        query.executeSQL05();
        query.executeSQL06();
        query.executeSQL07();
        query.executeSQL08();
        query.executeSQL09();
        query.executeSQL10();
        query.executeSQL11();
        query.executeSQL12();
        query.executeSQL13();
        query.executeSQL14();

        query.shutdown();
    }

    public void startup() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            String databaseURL = driverName + Configuration.instance.dataPath + "records.db";
            connection = DriverManager.getConnection(databaseURL, username, password);

            bufferedWriter = new BufferedWriter(new FileWriter(Configuration.instance.logPath + "query.log"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void writeLogfile(String message) {
        try {
            bufferedWriter.append(message).append("\n");
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }

    public String dump(ResultSet resultSet) {
        StringBuilder stringBuilder = new StringBuilder();

        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int maximumNumberColumns = resultSetMetaData.getColumnCount();
            int i;
            Object object;

            for (; resultSet.next(); ) {
                for (i = 0; i < maximumNumberColumns; ++i) {
                    object = resultSet.getObject(i + 1);
                    stringBuilder.append(object.toString() + " ");
                }
                stringBuilder.append(" \n");
            }

            return stringBuilder.toString();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }

        return "-1";
    }

    // count
    // count, where
    // count, where, in
    // count, where, not in
    // sum, where, in
    // avg, where, not in
    // id, where, in, order by desc limit
    // id, where, in, order by desc, order by asc
    // count, group by
    // count, where, group by
    // count, where, in, group by
    // count, where, not in, group by
    // sum, where, not in, in, group by
    // avg, where, in, in, group by

    public synchronized void queryDump(String sqlStatement) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sqlStatement);
            writeLogfile(sqlStatement);
            writeLogfile(dump(resultSet));
            statement.close();
        } catch (SQLException sqle) {
            System.out.println(sqle.getMessage());
        }
    }

    // count
    public void executeSQL01() {
        writeLogfile("--- query 01 (count)");
        String sqlStatement = "SELECT COUNT(*) FROM data";
        queryDump(sqlStatement);
    }

    // count, where
    public void executeSQL02() {
        writeLogfile("--- query 02 (count, where)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE source = 'A' AND destination = 'D'";
        queryDump(sqlStatement);
    }

    // count, where, in
    public void executeSQL03() {
        writeLogfile("--- query 03 (count, where, in)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE size = 'S' " +
                "AND source IN ('A','B','C') " +
                "AND weight >= 1000 AND weight <= 2500 " +
                "AND isExpress = 'true'";
        queryDump(sqlStatement);
    }

    // count, where, not in
    public void executeSQL04() {
        writeLogfile("--- query 04 (count, where, not in)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE size = 'L' AND source NOT IN ('A','B') " +
                "AND weight >= 1250 AND weight <= 3750 AND isExpress = 'false' AND isValue = 'true'";
        queryDump(sqlStatement);
    }

    // sum, where, in
    public void executeSQL05() {
        writeLogfile("--- query 05 (sum, where, in)");
        String sqlStatement = "SELECT SUM(weight) FROM data " +
                "WHERE size = 'L' AND source IN ('A','B') AND destination IN ('D','E') AND isValue = 'true'";
        queryDump(sqlStatement);
    }

    // avg, where, not in
    public void executeSQL06() {
        writeLogfile("--- query 06 (avg, where, not in)");
        String sqlStatement = "SELECT AVG(weight) FROM data " +
                "WHERE size IN ('S','M') " +
                "AND source NOT IN ('G','H') " +
                "AND isExpress ='false' " +
                "AND isValue = 'true'";
        queryDump(sqlStatement);
    }

    // id, where, in, order by desc limit
    public void executeSQL07() {
        writeLogfile("--- query 07 (id, where, in, order by desc limit)");
        String sqlStatement = "SELECT id FROM data " +
                "WHERE size = 'S' " +
                "AND destination IN ('D','E','F') " +
                "AND source = 'B' " +
                "AND weight >= 3500 " +
                "AND isExpress ='true' " +
                "AND isValue = 'true' " +
                "ORDER BY weight DESC LIMIT 3";
        queryDump(sqlStatement);
    }

    // id, where, in, order by desc, order by asc
    public void executeSQL08() {
        writeLogfile("--- query 08 (id, where, in, order by desc, order by asc)");
        String sqlStatement = "SELECT id FROM data " +
                "WHERE size = 'M' " +
                "AND destination = 'C' " +
                "AND source IN ('G','H','I','J') " +
                "AND weight <= 1500 " +
                "AND isExpress ='true' " +
                "AND isValue = 'true' " +
                "ORDER BY weight DESC,destination";
        queryDump(sqlStatement);
    }

    // count, group by
    public void executeSQL09() {
        writeLogfile("--- query 09 (count, group by)");
        String sqlStatement = "SELECT isExpress,COUNT(*) FROM data " +
                "GROUP BY isExpress";
        queryDump(sqlStatement);
    }

    // count, where, group by
    public void executeSQL10() {
        writeLogfile("--- query 10 (count, where, group by)");
        String sqlStatement = "SELECT size,COUNT(*) FROM data " +
                "WHERE isExpress = 'true' " +
                "AND isValue = 'false' " +
                "GROUP BY size";
        queryDump(sqlStatement);
    }

    // count, where, in, group by
    public void executeSQL11() {
        writeLogfile("--- query 11 (count, where, in, group by)");
        String sqlStatement = "SELECT destination,COUNT(*) FROM data " +
                "WHERE destination IN ('A','C') " +
                "AND isValue = 'false' " +
                "GROUP BY destination";
        queryDump(sqlStatement);
    }

    // count, where, not in, group by
    public void executeSQL12() {
        writeLogfile("--- query 12 (count, where, not in, group by)");
        String sqlStatement = "SELECT destination,COUNT(*) FROM data " +
                "WHERE source NOT IN ('B','C') AND isExpress = 'true' AND isValue = 'true' AND isFragile = 'true' " +
                "GROUP BY destination";
        queryDump(sqlStatement);
    }

    // sum, where, not in, in, group by
    public void executeSQL13() {
        writeLogfile("--- query 13 (sum, where, not in, in, group by)");
        String sqlStatement = "SELECT isValue,SUM(weight) FROM data " +
                "WHERE source = 'B' AND destination NOT IN ('D','E') AND weight >= 1000 AND weight <= 2750 " +
                "GROUP BY isValue";
        queryDump(sqlStatement);
    }

    // avg, where, in, in, group by
    public void executeSQL14() {
        writeLogfile("--- query 14 (avg, where, in, in, group by)");
        String sqlStatement = "SELECT destination,AVG(weight) FROM data " +
                "WHERE source IN ('A','B') AND destination IN ('C','D') AND weight >= 1250 AND isFragile = 'true' " +
                "GROUP BY destination";
        queryDump(sqlStatement);
    }

    public void shutdown() {
        try {
            Statement statement = connection.createStatement();
            statement.execute("SHUTDOWN");
            connection.close();
            bufferedWriter.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}