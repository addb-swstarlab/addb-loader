package kr.ac.yonsei.delab.addb_loader;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public class Config {
    @Getter @Setter String loader;
    @Getter @Setter Map<String, TableConfig> tables;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("==== Config ====\n")
                .append("1. Loader\n")
                .append(loader).append("\n")
                .append("2. Tables\n")
                .append(tables).append("\n")
                .append("----------------\n")
                .append("================");
        return builder.toString();
    }
}
