package kr.ac.yonsei.delab.addb_loader;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public class Config {
    @Getter @Setter Map<String, TableConfig> tables;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("==== Config ====\n")
                .append("1. Tables\n")
                .append(tables).append("\n")
                .append("----------------\n")
                .append("================");
        return builder.toString();
    }
}
