import java.io.IOException;
import java.util.ArrayList;

public class TestComputationSkew {

    public static void main(String[] args) throws IOException {
        String b = "result-for-computation"; //change to your spark log file
        ArrayList<ComputationSkewFeedback> array= new ComputationSkewDetector(4,b,b,0.9).getEntropyArray();

    }
}
