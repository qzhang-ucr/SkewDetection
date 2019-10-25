import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class DataSkewDetector {

    public static ArrayList<DataSkewResult> getDataSkewResults(int workerNum, String pathRead, String pathWrite, double threshold) throws IOException {

        ArrayList<DataSkewResult> array = new ArrayList<>();
        double[] memory = new double[workerNum];
        double totalMemory = 0.0;
        int maxStage = 0;

        String writeFile = pathWrite+"/result.txt";
        File resultF = new File(writeFile);
        FileOutputStream result_p = new FileOutputStream(resultF);
        OutputStreamWriter writer = new OutputStreamWriter(result_p, StandardCharsets.UTF_8);

        double[][] stageMemoryUse = new double[10000][workerNum];

        BufferedReader br = new BufferedReader(new FileReader(ReadFile.filePath(pathRead)));
        String line;

        while ((line = br.readLine()) != null) {
            //new DataSkewTools();

            if (DataSkewTools.stageReadBegin(line)) {
                int position = DataSkewTools.compareStage(line);

                int stage_number = DataSkewTools.transToNum(line, position);
                maxStage = stage_number;
                position = DataSkewTools.compareExecutor(line);
                int executorNumber = DataSkewTools.transToNum(line, position);
                double kb = (DataSkewTools.compareBit(line)*1.0)/1024;

                stageMemoryUse[stage_number][executorNumber]=stageMemoryUse[stage_number][executorNumber]+kb;
                memory[executorNumber]=memory[executorNumber]+kb;
                totalMemory=totalMemory+kb;

            }
        }

        double entropy_memory = DataSkewTools.computeEntropy(memory);
        writer.append("Entropy:").append(String.valueOf(entropy_memory)).append("\n");

        for (int i=0;i<workerNum;i++) {
            writer.append("Memory used for Worker").append(String.valueOf(i)).append(":").append(String.valueOf(memory[i])).append("KB").append("\n");
        }

        double[] entropyStage= new double[maxStage+1];

        for (int i = 0; i <= maxStage; i++) {
            double maxMemory=0;
            entropyStage[i] = DataSkewTools.computeEntropy(stageMemoryUse[i]);
            int useWorker=0;
            for (int j=0;j<workerNum;j++)
            {
                if (stageMemoryUse[i][j]>maxMemory)
                {
                    maxMemory=stageMemoryUse[i][j];
                }
                if (stageMemoryUse[i][j]>0)
                {
                    useWorker++;
                }
            }
            if (entropyStage[i] != -1) {
                writer.append("Entropy for Stage").append(String.valueOf(i)).append(":").append(String.valueOf(entropyStage[i])).append("\n");
                writer.append("Shuffle Read for each Worker in Stage").append(String.valueOf(i)).append(":").append(Arrays.toString(stageMemoryUse[i])).append("\n");
            } else {
                writer.append("No shuffling in Stage").append(String.valueOf(i)).append("\n");
            }
            DataSkewResult current = new DataSkewResult(entropyStage[i],i );

            if ((current.getEntropy()<(Math.log(useWorker)*threshold)) & (maxMemory>10))
            {
                array.add(current);
            }
        }


        writer.close();
        return array;
    }
}
