import java.io.IOException;
import java.util.ArrayList;

public class TestDataSkew {
    public static void main(String[] args) throws IOException {
        //new dataSkewDetected();
        String b= "/tmp/spark-events/";
        ArrayList<DataSkewFeedback> array = DataSkewDetector.getEntropy(6,b,b,0.99);
        for(DataSkewFeedback res:array)
        {
            System.out.println(res.getEntropy()+", "+res.getNumberOfStage());
        }
    }
}
