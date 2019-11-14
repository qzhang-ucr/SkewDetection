import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class ComputationSkewDetector {

    private ArrayList<ComputationSkewFeedback> computationSkewFeedback = new ArrayList<>();

    public ArrayList<ComputationSkewFeedback> getEntropyArray() {
        return this.computationSkewFeedback;
    }

    public void setEntropyArray (ArrayList<ComputationSkewFeedback> entropyArray) {
        this.computationSkewFeedback = entropyArray;
    }

    public ComputationSkewDetector(int workerNum, String pathRead, String pathWrite, double threshold) throws IOException
    {
        int maxStage = 0;

        String writeFile = pathWrite+"/result_computation_skew.txt";
        File resultFile = new File(writeFile);
        FileOutputStream result_p = new FileOutputStream(resultFile);
        OutputStreamWriter writer = new OutputStreamWriter(result_p, StandardCharsets.UTF_8);

        BufferedReader br = new BufferedReader(new FileReader(ReadLogFile.filePath(pathRead)));
        //System.out.println(ReadLogFile.filePath(pathRead));
        String line;

        double[] stageTimePerRecord = new double[workerNum];
        int[][] stageTime = new int[10000][workerNum];
        int[][] stageRecord = new int[10000][workerNum];
        while ((line = br.readLine()) != null) {

            //System.out.println(line);

            if (ComputationSkewTools.stageReadBegin(line)) {
                //System.out.println(line);

                int position = ComputationSkewTools.compareStage(line);
                int stageNumber = ComputationSkewTools.transToNum(line, position);
                maxStage = stageNumber;

                position = ComputationSkewTools.compareExecutor(line);
                int executorNumber = ComputationSkewTools.transToNum(line, position);
                int timeUsage = ComputationSkewTools.timeCalculate(line);
                int recordUsage = ComputationSkewTools.recordCalculate(line);


                stageTime[stageNumber][executorNumber] += timeUsage;
                stageRecord[stageNumber][executorNumber] += recordUsage;
            }
        }

        double[] entropyStage= new double[maxStage+1];

        //System.out.println(maxStage);
        for (int i = 0; i <= maxStage; i++) {

            for (int j=0;j<workerNum;j++)
            {
                if (stageRecord[i][j]>0) {stageTimePerRecord[j]=stageTime[i][j]*1.0/stageRecord[i][j];}
                else {stageTimePerRecord[j]=0;}
            }

            entropyStage[i] = DataSkewTools.computeEntropy(stageTimePerRecord);

            int useWorker=0;

            for (int j = 0; j < workerNum; j++)
            {
                if (stageTimePerRecord[j]>0)
                {
                    useWorker++;
                }
            }
            writer.append("Entropy for Stage").append(String.valueOf(i)).append(":").append(String.valueOf(entropyStage[i])).append("\n");
            writer.append("Stage for").append(String.valueOf(i)).append(Arrays.toString(stageTimePerRecord)).append("\n");

            ComputationSkewFeedback current = new ComputationSkewFeedback(entropyStage[i],i);

            if ((current.getEntropy()<(Math.log(useWorker)*threshold)))
            {
                this.computationSkewFeedback.add(current);
                System.out.println(current.getNumberOfStage());
                System.out.println(current.getEntropy());
            }
        }

        writer.close();
    }
}
