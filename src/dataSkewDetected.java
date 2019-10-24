import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class dataSkewDetected {

    public static ArrayList<dataSkewResult> show_entropy(int workerNum, String pathRead, String pathWrite, double threshold) throws IOException {

        ArrayList<dataSkewResult> array = new ArrayList<>();
        double[] memory = new double[workerNum];
        double totalMemory = 0.0;
        int maxStage = 0;

        String writeFile = pathWrite+"/result.txt";
        File resultF = new File(writeFile);
        FileOutputStream result_p = new FileOutputStream(resultF);
        OutputStreamWriter writer = new OutputStreamWriter(result_p, StandardCharsets.UTF_8);

        double[][] stageMemoryUse = new double[10000][workerNum];
        new readFile();

        BufferedReader br = new BufferedReader(new FileReader(readFile.filePath(pathRead)));
        String line;

        while ((line = br.readLine()) != null) {
            new dataSkewTools();

            if (new dataSkewTools().stageReadBegin(line)) {
                int position = new dataSkewTools().compareStage(line);

                int stage_number = dataSkewTools.transToNum(line, position);
                maxStage = stage_number;
                position = new dataSkewTools().compareExecutor(line);
                int executorNumber = dataSkewTools.transToNum(line, position);
                double kb = (new dataSkewTools().compareBit(line)*1.0)/1024;


                stageMemoryUse[stage_number][executorNumber]=stageMemoryUse[stage_number][executorNumber]+kb;
                memory[executorNumber]=memory[executorNumber]+kb;
                totalMemory=totalMemory+kb;

            }
        }

        double entropy_memory = new dataSkewTools().computeEntropy(memory);
        writer.append("Entropy:").append(String.valueOf(entropy_memory)).append("\n");

        for (int i=0;i<workerNum;i++) {
            writer.append("Memory used for Worker").append(String.valueOf(i)).append(":").append(String.valueOf(memory[i])).append("KB").append("\n");
        }

        double[] entropyStage= new double[maxStage+1];

        for (int i = 0; i <= maxStage; i++) {
            double maxMemory=0;
            entropyStage[i] = new dataSkewTools().computeEntropy(stageMemoryUse[i]);
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
            dataSkewResult Current = new dataSkewResult();
            Current.entropy=entropyStage[i];
            Current.NumberOfStage=i;
            if ((Current.entropy<(Math.log(useWorker)*threshold)) & (maxMemory>10))
            {
                array.add(Current);
            }
        }


        writer.close();
        return array;
    }
}
