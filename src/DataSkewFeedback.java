
public class DataSkewFeedback {

    private double entropy;
    private int numberOfStage;

    public DataSkewFeedback(double entropy, int numberOfStage)
    {
        this.entropy = entropy;
        this.numberOfStage = numberOfStage;
    }

    public double getEntropy()
    {

        return this.entropy;
    }

    public int getNumberOfStage()
    {

        return this.numberOfStage;
    }

    public void setEntropy(double entropy)
    {

        this.entropy = entropy;
    }

    public void setNumberOfStage(int numberOfStage)
    {

        this.numberOfStage = numberOfStage;
    }

    @Override
    public String toString() {
        return Double.toString(entropy)+", "+Integer.toString(numberOfStage);
    }
}
