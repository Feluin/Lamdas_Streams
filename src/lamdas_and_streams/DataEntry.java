package lamdas_and_streams;

public class DataEntry {
    private int id;
    private char size;
    private double weight;
    private char source;
    private char destination;
    private String isExpress;
    private String isValue;
    private String isFragile;

    public DataEntry(int id, char size, double weight, char source, char destination, String isExpress, String isValue, String isFragile) {
        this.id = id;
        this.size = size;
        this.weight = weight;
        this.source = source;
        this.destination = destination;
        this.isExpress = isExpress;
        this.isValue = isValue;
        this.isFragile = isFragile;
    }

    public int getId() {
        return id;
    }

    public char getSize() {
        return size;
    }

    public double getWeight() {
        return weight;
    }

    public char getSource() {
        return source;
    }

    public char getDestination() {
        return destination;
    }

    public String getIsExpress() {
        return isExpress;
    }

    public String getIsValue() {
        return isValue;
    }

    public String getIsFragile() {
        return isFragile;
    }
}
