
import java.util.regex.Pattern;
import java.util.regex.Matcher;



public class DataSkewTools {

    public static boolean stageReadBegin(String textLine)
    {
        Pattern pattern = Pattern.compile("\"Event\":\"SparkListenerTaskEnd\"");
        Matcher master = pattern.matcher(textLine);
        return master.find();
    }

    public static int compareBit(String textLine)
    {
        int sum = 0;
        Pattern pattern = Pattern.compile("\"Remote Bytes Read\":");
        Matcher master = pattern.matcher(textLine);

        if (!master.find()) {
            return -1;
        }

        sum = sum + transToNum(textLine, master.end());
        pattern = Pattern.compile("\"Local Bytes Read\":");
        master = pattern.matcher(textLine);
        master.find();
        sum = sum + transToNum(textLine, master.end());
        return sum;

    }

    public static int compareStage(String textLine)
    {
        Pattern pattern = Pattern.compile("\"Stage ID\":");
        Matcher master = pattern.matcher(textLine);
        if (!master.find()){
            return -1;
        }
        return master.end();
    }

    public static int compareExecutor(String textLine)
    {
        Pattern pattern = Pattern.compile("\"Executor ID\":");
        Matcher master = pattern.matcher(textLine);
        if (!master.find()){
            return -1;
        }
        return master.end();
    }

    public static double computeEntropy(double[] arr)
    {
        double entropy = 0;
        double sum = 0;
        for (double v : arr) {
            sum = sum + v;
        }
        if (sum ==0) return -1;
        for (double v:arr)
        {
            if (v!=0)
            {
                entropy = entropy-(v/sum)*(Math.log(v/sum));
            }
        }

        return entropy;
    }


    private static boolean judgeNumber(char x)
    {

        return (x >= '0') & (x <= '9');
    }

    public static int transToNum(String textLine, int startPoint)
    {
        if (startPoint==0) return 0;
        int i = startPoint;
        char[] b = textLine.toCharArray();
        int sum=0;

        while (!judgeNumber(b[i]))
        {
            i++;
        }
        while (judgeNumber(b[i]))
        {
            i++;
        }
        i = i - 1;
        int k = 1;

        while (judgeNumber(b[i]))
        {
            sum = sum + k * ( b[i] - '0');
            k = k * 10;
            i = i - 1;
        }

        return sum;

    }


}
