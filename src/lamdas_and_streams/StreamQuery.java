package lamdas_and_streams;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class StreamQuery implements IQuery {

    private List<DataEntry> dataEntryList = new ArrayList<>();

    public static void main(String... args) {
        StreamQuery query = new StreamQuery();
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
        Dataimport dataimport = new Dataimport("./data/records.csv");
        dataEntryList = dataimport.importCSVFile();
    }

    private void shutdown() {
    }

    @Override
    public void executeSQL01() {
        writeLogfile("--- query 01 (count)");
        String sqlStatement = "SELECT COUNT(*) FROM data";
        System.out.println(dataEntryList.stream().count());

    }

    // count, where
    public void executeSQL02() {
        writeLogfile("--- query 02 (count, where)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE source = 'A' AND destination = 'D'";
        System.out.println(dataEntryList.stream().filter(dataEntry -> dataEntry.getSource() == 'A' && dataEntry.getDestination() == 'D').count());

    }

    // count, where, in
    public void executeSQL03() {
        writeLogfile("--- query 03 (count, where, in)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE size = 'S' " +
                "AND source IN ('A','B','C') " +
                "AND weight >= 1000 AND weight <= 2500 " +
                "AND isExpress = 'true'";
        System.out.println(dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'S' &&
                        (dataEntry.getSource() == 'A' || dataEntry.getSource() == 'B' || dataEntry.getSource() == 'C') &&
                        dataEntry.getWeight() >= 1000 && dataEntry.getWeight() <= 2500 &&
                        dataEntry.getIsExpress().equals("true")).count());

    }

    // count, where, not in
    public void executeSQL04() {
        writeLogfile("--- query 04 (count, where, not in)");
        String sqlStatement = "SELECT COUNT(*) FROM data " +
                "WHERE size = 'L' AND source NOT IN ('A','B') " +
                "AND weight >= 1250 AND weight <= 3750 AND isExpress = 'false' AND isValue = 'true'";
        System.out.println(dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'L' &&
                        !(dataEntry.getSource() == 'A' || dataEntry.getSource() == 'B') &&
                        dataEntry.getWeight() >= 1250 && dataEntry.getWeight() <= 3750 &&
                        dataEntry.getIsExpress().equals("false") && dataEntry.getIsValue().equals("true")).count());

    }

    // sum, where, in
    public void executeSQL05() {
        writeLogfile("--- query 05 (sum, where, in)");
        String sqlStatement = "SELECT SUM(weight) FROM data " +
                "WHERE size = 'L' AND source IN ('A','B') AND destination IN ('D','E') AND isValue = 'true'";
        System.out.println(dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'L' &&
                        (dataEntry.getSource() == 'A' || dataEntry.getSource() == 'B') &&
                        (dataEntry.getDestination() == 'D' || dataEntry.getDestination() == 'E') &&
                        dataEntry.getIsValue().equals("true")).mapToDouble(DataEntry::getWeight).sum());

    }

    // avg, where, not in
    public void executeSQL06() {
        writeLogfile("--- query 06 (avg, where, not in)");
        String sqlStatement = "SELECT AVG(weight) FROM data " +
                "WHERE size IN ('S','M') " +
                "AND source NOT IN ('G','H') " +
                "AND isExpress ='false' " +
                "AND isValue = 'true'";
        System.out.println(dataEntryList.stream().filter(
                dataEntry -> (dataEntry.getSize() == 'S' || dataEntry.getSize() == 'S') &&
                        !(dataEntry.getSource() == 'G' || dataEntry.getSource() == 'H') &&
                        dataEntry.getIsExpress().equals("false") &&
                        dataEntry.getIsValue().equals("true"))
                .mapToDouble(DataEntry::getWeight).average().getAsDouble());


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
        List<Integer> idList = dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'S' &&
                        (dataEntry.getDestination() == 'D' || dataEntry.getDestination() == 'E' || dataEntry.getDestination() == 'F') &&
                        dataEntry.getSource() == 'B' &&
                        dataEntry.getWeight() >= 3500 &&
                        dataEntry.getIsExpress().equals("true") &&
                        dataEntry.getIsValue().equals("true"))
                .sorted(Comparator.comparingDouble(DataEntry::getWeight).reversed()).limit(3).
                        mapToInt(DataEntry::getId).boxed().collect(Collectors.toList());
        idList.forEach(integer -> System.out.print(integer + ","));
        System.out.println();

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
        List<Integer> idList = dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'M' &&
                        (dataEntry.getDestination() == 'C') &&
                        (dataEntry.getSource() == 'G' || dataEntry.getSource() == 'H' || dataEntry.getSource() == 'I' || dataEntry.getSource() == 'J') &&
                        dataEntry.getWeight() <= 1500 &&
                        dataEntry.getIsExpress().equals("true") &&
                        dataEntry.getIsValue().equals("true"))
                .sorted(Comparator.comparingDouble(DataEntry::getWeight).reversed().thenComparing(DataEntry::getDestination)).
                        mapToInt(DataEntry::getId).boxed().collect(Collectors.toList());
        idList.forEach(integer -> System.out.print(integer + ","));
        System.out.println();

    }

    // count, group by
    public void executeSQL09() {
        writeLogfile("--- query 09 (count, group by)");
        String sqlStatement = "SELECT isExpress,COUNT(*) FROM data " +
                "GROUP BY isExpress";
        Map<String, Long> collect = dataEntryList.stream().map(DataEntry::getIsExpress).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        collect.forEach((s, aLong) -> System.out.println(s + "|" + aLong));
    }


    // count, where, group by
    public void executeSQL10() {
        writeLogfile("--- query 10 (count, where, group by)");
        String sqlStatement = "SELECT size,COUNT(*) FROM data " +
                "WHERE isExpress = 'true' " +
                "AND isValue = 'false' " +
                "GROUP BY size";

        Map<Character, Long> collect = dataEntryList.stream().filter(dataEntry -> dataEntry.getIsExpress().equals("true") &&
                dataEntry.getIsValue().equals("false")).map(DataEntry::getSize).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        collect.forEach((s, aLong) -> System.out.println(s + "|" + aLong));

    }

    // count, where, in, group by
    public void executeSQL11() {
        writeLogfile("--- query 11 (count, where, in, group by)");
        String sqlStatement = "SELECT destination,COUNT(*) FROM data " +
                "WHERE destination IN ('A','C') " +
                "AND isValue = 'false' " +
                "GROUP BY destination";
        Map<Character, Long> collect = dataEntryList.stream().filter(dataEntry -> (dataEntry.getDestination() == 'A' || dataEntry.getDestination() == 'C' )
                && dataEntry.getIsValue().equals("false"))
                .map(DataEntry::getDestination).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        collect.forEach((s, aLong) -> System.out.println(s + "|" + aLong));

    }

    // count, where, not in, group by
    public void executeSQL12() {
        writeLogfile("--- query 12 (count, where, not in, group by)");
        String sqlStatement = "SELECT destination,COUNT(*) FROM data " +
                "WHERE source NOT IN ('B','C') AND isExpress = 'true' AND isValue = 'true' AND isFragile = 'true' " +
                "GROUP BY destination";
        Map<Character, Long> collect = dataEntryList.stream().filter(dataEntry -> (dataEntry.getDestination() == 'A' || dataEntry.getDestination() == 'C' )
                && dataEntry.getIsValue().equals("false"))
                .map(DataEntry::getDestination).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        collect.forEach((s, aLong) -> System.out.println(s + "|" + aLong));


    }

    // sum, where, not in, in, group by
    public void executeSQL13() {
        writeLogfile("--- query 13 (sum, where, not in, in, group by)");
        String sqlStatement = "SELECT isValue,SUM(weight) FROM data " +
                "WHERE source = 'B' AND destination NOT IN ('D','E') AND weight >= 1000 AND weight <= 2750 " +
                "GROUP BY isValue";

    }

    // avg, where, in, in, group by
    public void executeSQL14() {
        writeLogfile("--- query 14 (avg, where, in, in, group by)");
        String sqlStatement = "SELECT destination,AVG(weight) FROM data " +
                "WHERE source IN ('A','B') AND destination IN ('C','D') AND weight >= 1250 AND isFragile = 'true' " +
                "GROUP BY destination";
    }


    private void writeLogfile(String s) {
        System.out.println(s);
    }

}
