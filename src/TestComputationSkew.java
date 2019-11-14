import java.io.IOException;
import java.util.ArrayList;

public class TestComputationSkew {

    public static void main(String[] args) throws IOException {
        String b = "chapter5/";
        ArrayList<ComputationSkewFeedback> array= new ComputationSkewDetector(4,b,b,0.9).getEntropyArray();

    }
}
