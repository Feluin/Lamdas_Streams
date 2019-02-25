package las;

public enum Configuration {
    instance;

    public MersenneTwister randomNumberGenerator = new MersenneTwister(System.currentTimeMillis());

    public String fileSeparator = System.getProperty("file.separator");
    public String lineSeparator = System.getProperty("line.separator");

    public String dataPath = "data" + fileSeparator;
    public String logPath = "log" + fileSeparator;

    public int maximumNumberOfRecords = 1000000;

    public double ratioExpress = 0.08;
    public double ratioValue = 0.01;
    public double ratioFragile = 0.03;
}