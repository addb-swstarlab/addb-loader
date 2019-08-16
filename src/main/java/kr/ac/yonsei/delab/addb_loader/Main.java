package kr.ac.yonsei.delab.addb_loader;

import kr.ac.yonsei.delab.addb_loader.jedis_loader.JedisLoader;
import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class Main {
    public static void main(String[] args) {
        /* Command type : ./Loader.jar -c config.yaml -t lineitem -h 255.255.255.0 -p 8000 */
        /* Parses arguments... */
        Option configOption = new Option(
                "c", "config", true, "Config file path");
        configOption.setRequired(true);
        Option tableOption = new Option(
                "t", "table", true, "Table name in config file");
        tableOption.setRequired(true);
        Option hostOption = new Option(
                "h", "host", true, "Host of Redis master node");
        hostOption.setRequired(true);
        Option portOption = new Option(
                "p", "port", true, "Port of Redis master node");
        portOption.setRequired(true);
        Options options = new Options()
                .addOption(configOption)
                .addOption(tableOption)
                .addOption(hostOption)
                .addOption(portOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("loader", options);
            throw new RuntimeException();
        }

        String configFileName = cmd.getOptionValue("config");
        String tableName = cmd.getOptionValue("table");
        String host = cmd.getOptionValue("host");
        String port = cmd.getOptionValue("port");

        /* Parses config file... */
        Yaml yaml = new Yaml(new Constructor(Config.class));
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(new File(configFileName));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Config file is not founded");
        }
        Global.config = yaml.loadAs(inputStream, Config.class);
        Global.host = host;
        Global.port = Integer.valueOf(port);

        TableConfig tableConfig = Global.config.tables.get(tableName);
        if (tableConfig == null) {
            throw new RuntimeException("Table name is not founded on config.yaml");
        }

        System.out.println(Global.config);

        /* Start Loader... */
        Loader loader;
        if (Global.config.getLoader().equalsIgnoreCase("jedis")) {
            loader = JedisLoader.create(
                    Integer.toString(tableConfig.getId()),
                    tableConfig.getFile(),
                    tableConfig.getPartitionCols()
            );
        } else if (Global.config.getLoader().equalsIgnoreCase("lettuce")) {
            // TODO(totoro): Implements Lettuce Loader
            System.out.println("TODO(totoro): Implements Lettuce Loader...");
            loader = JedisLoader.create(
                    Integer.toString(tableConfig.getId()),
                    tableConfig.getFile(),
                    tableConfig.getPartitionCols()
            );
        } else {
            throw new RuntimeException("Loader must be either 'jedis' or 'lettuce'.");
        }
        loader.start();
    }
}