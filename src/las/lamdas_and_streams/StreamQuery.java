package las.lamdas_and_streams;

import las.Configuration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StreamQuery implements ILaSQuery {

    private List<DataEntry> dataEntryList = new ArrayList<>();
    private BufferedWriter logFileWriter;

    public static void main(String... args) {
        StreamQuery query = new StreamQuery();
        query.startup();

        Long execute01 = query.execute01();
        query.writeLogfile("Result of 1:" + execute01);
        Long execute02 = query.execute02();
        query.writeLogfile("Result of 2:" + execute02);
        Long execute03 = query.execute03();
        query.writeLogfile("Result of 3:" + execute03);
        Long execute04 = query.execute04();
        query.writeLogfile("Result of 4:" + execute04);
        Double execute05 = query.execute05();
        query.writeLogfile("Result of 5:" + execute05);
        Double execute06 = query.execute06();
        query.writeLogfile("Result of 6:" + execute06);
        List<Integer> execute07 = query.execute07();
        query.writeLogfile("Result of 7:");
        execute07.forEach(integer -> query.writeLogfile(integer.toString()));
        List<Integer> execute08 = query.execute08();
        query.writeLogfile("Result of 8:");
        execute08.forEach(integer -> query.writeLogfile(integer.toString()));
        Map<String, Long> execute09 = query.execute09();
        query.writeLogfile("Result of 9:" );
        execute09.forEach((s, aLong) -> query.writeLogfile(s+"|"+aLong));
        Map<Character, Long> execute10 = query.execute10();
        query.writeLogfile("Result of 10:" );
        execute10.forEach((s, aLong) -> query.writeLogfile(s+"|"+aLong));
        Map<Character, Long> execute11 = query.execute11();
        query.writeLogfile("Result of 11:");
        execute11.forEach((s, aLong) -> query.writeLogfile(s+"|"+aLong));
        Map<Character, Long> execute12 = query.execute12();
        query.writeLogfile("Result of 12:");
        execute12.forEach((s, aLong) -> query.writeLogfile(s+"|"+aLong));
        Map<String, Double> execute13 = query.execute13();
        query.writeLogfile("Result of 13:");
        execute13.forEach((s, aLong) -> query.writeLogfile(s+"|"+aLong));
        Map<Character, Double> execute14 = query.execute14();
        query.writeLogfile("Result of 14:" );
        execute14.forEach((s, aLong) -> query.writeLogfile(s+"|"+aLong));

        query.shutdown();
    }

    public void startup() {
        Dataimport dataimport = new Dataimport("./data/records.csv");
        try {
            logFileWriter = new BufferedWriter(new FileWriter(Configuration.instance.logPath + "lasquery.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
        dataEntryList = dataimport.importCSVFile();
    }

    private void shutdown() {
        try {
            logFileWriter.append("finished");
            logFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    //"SELECT COUNT(*) FROM data";
    @Override
    public Long execute01() {
        writeLogfile("--- query 01 (count)");

        return dataEntryList.stream().count();

    }

    // count, where
    //SELECT COUNT(*) FROM data WHERE source = 'A' AND destination = 'D'
    public Long execute02() {
        writeLogfile("--- query 02 (count, where)");
        return dataEntryList.stream().filter(dataEntry -> dataEntry.getSource() == 'A' && dataEntry.getDestination() == 'D').count();

    }

    // count, where, in

    //SELECT COUNT(*) FROM data WHERE size = 'S' AND source IN ('A','B','C') " +AND weight >= 1000 AND weight <= 2500 AND isExpress = 'true'
    public Long execute03() {
        writeLogfile("--- query 03 (count, where, in)");
        return dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'S' &&
                        (dataEntry.getSource() == 'A' || dataEntry.getSource() == 'B' || dataEntry.getSource() == 'C') &&
                        dataEntry.getWeight() >= 1000 && dataEntry.getWeight() <= 2500 &&
                        dataEntry.getIsExpress().equals("true")).count();

    }

    // count, where, not in
    //SELECT COUNT(*) FROM data WHERE size = 'L' AND source NOT IN ('A','B') AND weight >= 1250 AND weight <= 3750 AND isExpress = 'false' AND isValue = 'true'
    public Long execute04() {
        writeLogfile("--- query 04 (count, where, not in)");
        return dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'L' &&
                        !(dataEntry.getSource() == 'A' || dataEntry.getSource() == 'B') &&
                        dataEntry.getWeight() >= 1250 && dataEntry.getWeight() <= 3750 &&
                        dataEntry.getIsExpress().equals("false") && dataEntry.getIsValue().equals("true")).count();

    }

    // sum, where, in"SELECT SUM(weight) FROM data " +
    //                "WHERE size = 'L' AND source IN ('A','B') AND destination IN ('D','E') AND isValue = 'true'";
    public Double execute05() {
        writeLogfile("--- query 05 (sum, where, in)");

        return dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'L' &&
                        (dataEntry.getSource() == 'A' || dataEntry.getSource() == 'B') &&
                        (dataEntry.getDestination() == 'D' || dataEntry.getDestination() == 'E') &&
                        dataEntry.getIsValue().equals("true")).mapToDouble(DataEntry::getWeight).sum();

    }

    // avg, where, not in
    //SELECT AVG(weight) FROM data WHERE size IN ('S','M') AND source NOT IN ('G','H') AND isExpress ='false' AND isValue = 'true'
    public Double execute06() {
        writeLogfile("--- query 06 (avg, where, not in)");
        return dataEntryList.stream().filter(
                dataEntry -> (dataEntry.getSize() == 'S' || dataEntry.getSize() == 'S') &&
                        !(dataEntry.getSource() == 'G' || dataEntry.getSource() == 'H') &&
                        dataEntry.getIsExpress().equals("false") &&
                        dataEntry.getIsValue().equals("true"))
                .mapToDouble(DataEntry::getWeight).average().getAsDouble();


    }

    // id, where, in, order by desc limit
    //SELECT id FROM data WHERE size = 'S' AND destination IN ('D','E','F') AND source = 'B' AND weight >= 3500  AND isExpress ='true' AND isValue = 'true' ORDER BY weight DESC LIMIT 3
    public List<Integer> execute07() {
        writeLogfile("--- query 07 (id, where, in, order by desc limit)");

        return dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'S' &&
                        (dataEntry.getDestination() == 'D' || dataEntry.getDestination() == 'E' || dataEntry.getDestination() == 'F') &&
                        dataEntry.getSource() == 'B' &&
                        dataEntry.getWeight() >= 3500 &&
                        dataEntry.getIsExpress().equals("true") &&
                        dataEntry.getIsValue().equals("true"))
                .sorted(Comparator.comparingDouble(DataEntry::getWeight).reversed()).limit(3).
                        mapToInt(DataEntry::getId).boxed().collect(Collectors.toList());


    }

    // id, where, in, order by desc, order by asc
    //SELECT id FROM data  WHERE size = 'M' AND destination = 'C' AND source IN ('G','H','I','J')
    // AND weight <= 1500 AND isExpress ='true' AND isValue = 'true' ORDER BY weight DESC,destination";
    public List<Integer> execute08() {
        writeLogfile("--- query 08 (id, where, in, order by desc, order by asc)");
        return dataEntryList.stream().filter(
                dataEntry -> dataEntry.getSize() == 'M' &&
                        (dataEntry.getDestination() == 'C') &&
                        (dataEntry.getSource() == 'G' || dataEntry.getSource() == 'H' || dataEntry.getSource() == 'I' || dataEntry.getSource() == 'J') &&
                        dataEntry.getWeight() <= 1500 &&
                        dataEntry.getIsExpress().equals("true") &&
                        dataEntry.getIsValue().equals("true"))
                .sorted(Comparator.comparingDouble(DataEntry::getWeight).reversed().thenComparing(DataEntry::getDestination)).
                        mapToInt(DataEntry::getId).boxed().collect(Collectors.toList());


    }

    // count, group by
    //"SELECT isExpress,COUNT(*) FROM data GROUP BY isExpress
    public Map<String, Long> execute09() {
        writeLogfile("--- query 09 (count, group by)");
        return dataEntryList.stream().map(DataEntry::getIsExpress).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    }


    // count, where, group by
    //SELECT size,COUNT(*) FROM data WHERE isExpress = 'true' AND isValue = 'false' GROUP BY size
    public Map<Character, Long> execute10() {
        writeLogfile("--- query 10 (count, where, group by)");


        return dataEntryList.stream().filter(dataEntry -> dataEntry.getIsExpress().equals("true") &&
                dataEntry.getIsValue().equals("false")).map(DataEntry::getSize).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    }

    // count, where, in, group by
    public Map<Character, Long> execute11() {
        writeLogfile("--- query 11 (count, where, in, group by)");
        String Statement = "SELECT destination,COUNT(*) FROM data " +
                "WHERE destination IN ('A','C') " +
                "AND isValue = 'false' " +
                "GROUP BY destination";
        return dataEntryList.stream().filter(dataEntry -> (dataEntry.getDestination() == 'A' || dataEntry.getDestination() == 'C')
                && dataEntry.getIsValue().equals("false"))
                .map(DataEntry::getDestination).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));


    }

    // count, where, not in, group by
    //"SELECT destination,COUNT(*) FROM data WHERE source NOT IN ('B','C') AND isExpress = 'true' AND isValue = 'true' AND isFragile = 'true' GROUP BY destination";
    public Map<Character, Long> execute12() {
        writeLogfile("--- query 12 (count, where, not in, group by)");
        return dataEntryList.stream().filter(dataEntry -> !(dataEntry.getSource() == 'B' || dataEntry.getDestination() == 'C')
                && dataEntry.getIsExpress().equals("true")
                && dataEntry.getIsValue().equals("true")
                && dataEntry.getIsFragile().equals("true"))
                .map(DataEntry::getDestination).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));


    }

    // sum, where, not in, in, group by
    //SELECT isValue,SUM(weight) FROM data WHERE source = 'B' AND destination NOT IN ('D','E') AND weight >= 1000 AND weight <= 2750 GROUP BY isValue
    public Map<String, Double> execute13() {
        writeLogfile("--- query 13 (sum, where, not in, in, group by)");
        return dataEntryList.stream().filter(dataEntry ->
                dataEntry.getSource() == 'B' &&
                        !(dataEntry.getDestination() == 'D' || dataEntry.getDestination() == 'E')
                        && dataEntry.getWeight() >= 1000 && dataEntry.getWeight() <= 2750)
                .collect(Collectors.groupingBy(DataEntry::getIsValue, Collectors.summingDouble(DataEntry::getWeight)));


    }

    // avg, where, in, in, group by
    // "SELECT destination,AVG(weight) FROM data WHERE source IN ('A','B') AND destination IN ('C','D') AND weight >= 1250 AND isFragile = 'true' GROUP BY destination
    public Map<Character, Double> execute14() {
        writeLogfile("--- query 14 (avg, where, in, in, group by)");
        return dataEntryList.stream().filter(dataEntry ->
                (dataEntry.getSource() == 'A' || dataEntry.getSource() == 'B') &&
                        (dataEntry.getDestination() == 'C' || dataEntry.getDestination() == 'D')
                        && dataEntry.getWeight() >= 1250 && dataEntry.getIsFragile().equals("true"))
                .collect(Collectors.groupingBy(DataEntry::getDestination, Collectors.averagingDouble(DataEntry::getWeight)));

    }


    private void writeLogfile(String message) {
        System.out.println(message);
        try {
            logFileWriter.append(message).append("\n");
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }

}
