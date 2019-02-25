public class Record {
    private int id;
    private String size;
    private int weight;
    private String source;
    private String destination;
    private boolean isExpress;
    private boolean isValue;
    private boolean isFragile;

    public Record(int id, String size, int weight, String source, String destination,
                  boolean isExpress, boolean isValue, boolean isFragile) {
        this.id = id;
        this.size = size;
        this.weight = weight;
        this.source = source;
        this.destination = destination;
        this.isExpress = isExpress;
        this.isValue = isValue;
        this.isFragile = isFragile;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(id).append(";").append(size).append(";").append(weight).append(";");
        stringBuilder.append(source).append(";").append(destination).append(";");
        stringBuilder.append(isExpress).append(";").append(isValue).append(";").append(isFragile);
        return stringBuilder.toString();
    }
}