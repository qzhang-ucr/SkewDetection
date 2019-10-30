import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ComputationSkewTools extends DataSkewTools{

    private static int findStartTime(String textLine)
    {
        Pattern pattern = Pattern.compile("\"Launch Time\":");
        Matcher master = pattern.matcher(textLine);
        if (!master.find()) {System.out.println("Error: No Launch Time find:");}
        return master.end()+5; //To ensure time is less than Max_Value of int
    }

    private static int findEndTime(String textLine)
    {
        Pattern pattern = Pattern.compile("\"Finish Time\":");
        Matcher master = pattern.matcher(textLine);
        if (!master.find()) {System.out.println("Error: No Finish Time find:");}
        return master.end()+5; //To ensure time is less than Max_Value of int
    }

    public static int timeCalculate(String textLine)
    {
        int startTime = transToNum(textLine,findStartTime(textLine));
        int endTime = transToNum(textLine, findEndTime(textLine));
        return endTime-startTime;
    }

    private static int findRecord(String textLine)
    {
        Pattern pattern = Pattern.compile("\"Total Records Read\":");
        Matcher master = pattern.matcher(textLine);
        if (!master.find()) {System.out.println("Error: No Records find:");};
        return master.end();
    }

    public static int recordCalculate(String textLine)
    {
        return transToNum(textLine,findRecord(textLine));
    }

}
