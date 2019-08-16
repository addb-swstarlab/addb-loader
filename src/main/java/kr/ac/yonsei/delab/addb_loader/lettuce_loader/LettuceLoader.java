package kr.ac.yonsei.delab.addb_loader.lettuce_loader;

import kr.ac.yonsei.delab.addb_loader.Loader;

import java.io.*;

public class LettuceLoader implements Loader {
    private String tableId;
    private String fileName;
    private String[] partitionColumns;

    public static Loader create(String tableId, String fileName, String[] partitionColumns) {
        return new LettuceLoader(tableId, fileName, partitionColumns);
    }

    public LettuceLoader(String tableId, String fileName, String[] partitionColumns) {
        this.tableId = tableId;
        this.fileName = fileName;
        this.partitionColumns = partitionColumns;
    }

    public void start() {
        double total_time = 0;
        double total_time_start, total_time_end;
        double lettuce_time = 0;
        double lettuce_time_start, lettuce_time_end;

        total_time_start = System.currentTimeMillis();
        BufferedReader fileReader;
        try {
            fileReader = new BufferedReader(new FileReader(new File(fileName)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        try {
            String line = null;
            int colCnt;
            while ((line = fileReader.readLine()) != null) {
                String[] colValues = line.split("\\|");
                colCnt = colValues.length;
                lettuce_time_start = System.currentTimeMillis();
                String dataKey = createDataKey(colValues);
                lettuce_time_end = System.currentTimeMillis();
                lettuce_time += lettuce_time_end - lettuce_time_start;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        try {
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        total_time_end = System.currentTimeMillis();
        total_time = total_time_end - total_time_start;
    }

    private String createDataKey(String[] colValues) {
        StringBuilder builder = new StringBuilder();
        builder.append("D:{")
                .append(tableId)
                .append(":");
        for (int i = 0; i < partitionColumns.length; ++i) {
            int partitionCol = Integer.parseInt(partitionColumns[i]);
            builder.append(partitionCol);
            builder.append(":");
            builder.append(colValues[partitionCol - 1]);
            if (i != partitionColumns.length - 1) {
                builder.append(":");
            }
        }
        builder.append("}");
        return builder.toString();
    }
}
