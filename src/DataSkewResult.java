public class DataSkewResult {
    private double entropy;
    private int numberOfStage;

    public DataSkewResult(double entropy, int numberOfStage)
    {
        this.entropy = entropy;
        this.numberOfStage = numberOfStage;
    }

    double getEntropy()
    {
        return this.entropy;
    }
    int getNumberOfStage()
    {
        return this.numberOfStage;
    }
    void setEntropy(double entropy)
    {
        this.entropy = entropy;
    }
    void setNumberOfStage(int numberOfStage)
    {
        this.numberOfStage = numberOfStage;
    }
}
