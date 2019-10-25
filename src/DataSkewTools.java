
import java.util.regex.Pattern;
import java.util.regex.Matcher;



public class DataSkewTools {

    public static boolean stageReadBegin(String text_line)
    {
        Pattern p = Pattern.compile("\"Event\":\"SparkListenerTaskEnd\"");
        Matcher m = p.matcher(text_line);
        return m.find();
    }

    public static int compareBit(String text_line)
    {
        int sum=0;
        Pattern p = Pattern.compile("\"Remote Bytes Read\":");
        Matcher m = p.matcher(text_line);
        if (!m.find()) {return -1;}
        sum = sum + transToNum(text_line,m.end());
        p = Pattern.compile("\"Local Bytes Read\":");
        m=p.matcher(text_line);
        m.find();
        sum = sum + transToNum(text_line,m.end());
        return sum;

    }

    public static int compareStage(String text_line)
    {
        Pattern p = Pattern.compile("\"Stage ID\":");
        Matcher m = p.matcher(text_line);
        if (!m.find()){return -1;}
        return m.end();
    }
    public static int compareExecutor(String text_line)
    {
        Pattern p = Pattern.compile("\"Executor ID\":");
        Matcher m = p.matcher(text_line);
        if (!m.find()){return -1;}
        return m.end();
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

    static int transToNum(String text_line, int start_point)
    {
        if (start_point==0) return 0;
        int i = start_point;
        char[] b = text_line.toCharArray();
        int sum=0;

        while (!judgeNumber(b[i]))
        {
            i++;
        }
        while (judgeNumber(b[i]))
        {
            i++;
        }
        i=i-1;
        int k=1;

        while (judgeNumber(b[i]))
        {
            sum = sum+k*(b[i]-'0');
            k=k*10;
            i=i-1;
        }

        return sum;

    }


}
