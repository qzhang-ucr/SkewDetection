import java.io.IOException;
import java.util.ArrayList;

public class test_main {
    public static void main(String[] args) throws IOException {
        //new dataSkewDetected();
        String b= "/tmp/spark-events/";
        ArrayList<DataSkewResult> array = DataSkewDetector.getDataSkewResults(6,b,b,0.95);
        for(DataSkewResult res:array)
        {
            System.out.println(res.getEntropy()+", "+res.getNumberOfStage());
        }
    }
}
