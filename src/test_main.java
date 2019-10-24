import java.io.IOException;
import java.util.ArrayList;

public class test_main {
    public static void main(String[] args) throws IOException {
        new dataSkewDetected();
        String b= "/tmp/spark-events/";
        ArrayList<dataSkewResult> array;
        array = dataSkewDetected.show_entropy(6,b,b,0.95);
        System.out.println(array.get(2).entropy);
        System.out.println(array.get(2).NumberOfStage);
    }
}
