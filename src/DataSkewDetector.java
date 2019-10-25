import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class DataSkewDetector {

    private ArrayList<DataSkewFeedback> dataSkewFeedback = new ArrayList<>();

    public DataSkewDetector() {

    }

    public ArrayList<DataSkewFeedback> getEntropyArray() {
        return this.dataSkewFeedback;
    }

    public void setEntropyArray (ArrayList<DataSkewFeedback> entropyarray) {
        this.dataSkewFeedback = entropyarray;
    }

    public static ArrayList<DataSkewFeedback> getEntropy(int workerNum, String pathRead, String pathWrite, double threshold) throws IOException {

        ArrayList<DataSkewFeedback> array = new ArrayList<>();
        double[] memory = new double[workerNum];

        double totalMemory = 0.0;
        int maxStage = 0;

        String writeFile = pathWrite+"/result.txt";
        File resultFile = new File(writeFile);
        FileOutputStream result_p = new FileOutputStream(resultFile);
        OutputStreamWriter writer = new OutputStreamWriter(result_p, StandardCharsets.UTF_8);

        BufferedReader br = new BufferedReader(new FileReader(ReadLogFile.filePath(pathRead)));
        String line;

        double[][] stageMemoryUse = new double[10000][workerNum];
        while ((line = br.readLine()) != null) {

            if (DataSkewTools.stageReadBegin(line)) {

                int position = DataSkewTools.compareStage(line);
                int stageNumber = DataSkewTools.transToNum(line, position);
                maxStage = stageNumber;

                position = DataSkewTools.compareExecutor(line);
                int executorNumber = DataSkewTools.transToNum(line, position);
                double memoryUsage = (DataSkewTools.compareBit(line)*1.0)/1024;

                stageMemoryUse[stageNumber][executorNumber] = stageMemoryUse[stageNumber][executorNumber] + memoryUsage;
                memory[executorNumber] = memory[executorNumber] + memoryUsage;
                totalMemory = totalMemory + memoryUsage;

            }
        }

        double entropy_memory = DataSkewTools.computeEntropy(memory);
        writer.append("Entropy:").append(String.valueOf(entropy_memory)).append("\n");

        for (int i=0;i<workerNum;i++) {
            writer.append("Shuffle Read for Worker").append(String.valueOf(i)).append(":").append(String.valueOf(memory[i])).append("KB").append("\n");
        }

        double[] entropyStage= new double[maxStage+1];

        for (int i = 0; i <= maxStage; i++) {

            double maxMemory = 0;

            entropyStage[i] = DataSkewTools.computeEntropy(stageMemoryUse[i]);

            int useWorker=0;

            for (int j = 0; j < workerNum; j++)
            {
                if (stageMemoryUse[i][j] > maxMemory)
                {
                    maxMemory = stageMemoryUse[i][j];
                }
                if (stageMemoryUse[i][j]>0)
                {
                    useWorker++;
                }
            }

//            if (entropyStage[i] != -1) {
//                writer.append("Entropy for Stage").append(String.valueOf(i)).append(":").append(String.valueOf(entropyStage[i])).append("\n");
//                writer.append("Shuffle Read for each Worker in Stage").append(String.valueOf(i)).append(":").append(Arrays.toString(stageMemoryUse[i])).append("\n");
//            } else {
//                writer.append("No shuffling in Stage").append(String.valueOf(i)).append("\n");
//            }

            DataSkewFeedback current = new DataSkewFeedback(entropyStage[i],i );

            if ((current.getEntropy()<(Math.log(useWorker)*threshold)) & (maxMemory>10))
            {
                array.add(current);
            }
        }

        writer.close();
        return array;
    }
}
