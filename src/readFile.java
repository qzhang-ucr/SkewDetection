import java.io.File;


class readFile {
    static String filePath(String directoryPath)
    {
        String max="";
        File file=new File(directoryPath);
        if (file.isDirectory())
        {
            String[] fileList=file.list();
            assert fileList != null;
            for (String s : fileList) {
                if ((s.compareTo(max) > 0)&(!s.equals("result.txt"))) {
                    max = s;
                }
            }
        }
        else {
            System.out.println("Not a directory");
        }
        return directoryPath+max;
    }
}
