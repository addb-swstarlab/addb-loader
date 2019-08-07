package kr.ac.yonsei.delab.addb_loader;

import lombok.Getter;
import lombok.Setter;

public class TableConfig {
    @Getter @Setter private int id;
    @Getter @Setter private String file;
    @Getter @Setter private String[] partitionCols;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("id: ").append(id).append(" | ")
                .append("file: ").append(file).append(" | ")
                .append("partitionCols: [").append(String.join(",", partitionCols)).append("]");
        return builder.toString();
    }
}
