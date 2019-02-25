package las.lamdas_and_streams;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Dataimport {

    private String fileName;

    public Dataimport(String fileName){

        this.fileName = fileName;
    }

    public List<DataEntry> importCSVFile() {
        List<DataEntry> dataEntryList= new ArrayList<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strings = line.split(";");
                dataEntryList.add(createDataEntry(Integer.parseInt(strings[0]), strings[1], Double.parseDouble(strings[2]),
                        strings[3], strings[4], strings[5], strings[6], strings[7]));
            }
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
        return  dataEntryList;
    }

    private DataEntry createDataEntry(int id, String size, double weight, String source, String destination, String isExpress, String isValue, String isFragile) {
        char charsize =size.length()>0?size.toCharArray()[0]:' ';
        char charsource =source.length()>0?source.toCharArray()[0]:' ';
        char chardest =destination.length()>0?destination.toCharArray()[0]:' ';

        return new DataEntry(id,charsize,weight,charsource,chardest,isExpress,isValue, isFragile);
    }
}
