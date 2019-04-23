package com.github.anicolaspp.configuration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

public class ParseOptions implements Serializable {
    private Options options;
    private long rowCount;
    private String output;
    private String className;
    private int tasks;
    private int partitions;
    private String compressionType;
    private int variableSize;
    private String banner;
    private int showRows;
    private int rangeInt;
    private boolean affixRandom;
    private String outputFileFormat;
    private Map<String, String> dataSinkOptions;
    private Integer concurrency;
    
    public ParseOptions() {
        this.rowCount = 10;
        this.output = "/ParqGenOutput.parquet";
        this.className = "ParquetExample";
        this.tasks = 1;
        this.partitions = 1;
        this.compressionType = "uncompressed";
        this.variableSize = 100;
        this.showRows = 0;
        this.rangeInt = Integer.MAX_VALUE;
        this.affixRandom = false;
        this.outputFileFormat = "maprdb";
        this.dataSinkOptions = new Hashtable<>();
        this.concurrency = 100;
        
        options = new Options();
        
        options.addOption("c", "threads", true, "<long> total number of threads (default: " + this.concurrency + ")");
        options.addOption("r", "rows", true, "<long> total number of rows (default: " + this.rowCount + ")");
        options.addOption("o", "output", true, "<String> the output file name (default: " + this.output + ")");
        options.addOption("t", "tasks", true, "<int> number of tasks to generate this data (default: " + this.tasks + ")");
        options.addOption("p", "partitions", true, "<int> number of output partitions (default: " + this.partitions + ")");
        options.addOption("s", "size", true, "<int> any variable payload size, string or payload in IntPayload (default: "
                + this.variableSize + ")");
        options.addOption("R", "rangeInt", true, "<int> maximum int value, value for any Int column will be generated " +
                "between [0,rangeInt), (default: " + this.rangeInt + ")");
        options.addOption("S", "show", true, "<int> show <int> number of rows (default: " + this.showRows +
                ", zero means do not show)");
        options.addOption("C", "compress", true, "<String> compression type, valid values are: uncompressed, " +
                "snappy, gzip, lzo (default: "
                + this.compressionType + ")");
        options.addOption("f", "format", true, "<String> output format type (e.g., parquet (default), csv, etc.)");
        options.addOption("O", "options", true, "<str,str> key,value strings that will be passed to the data source of spark in writing." +
                " E.g., for parquet you may want to re-consider parquet.block.size. The default is 128MB (the HDFS block size). ");
        
        this.banner = "\n" +
                " __  __             ____    ____        _           ____            \n" +
                "|  \\/  | __ _ _ __ |  _ \\  |  _ \\  __ _| |_ __ _   / ___| ___ _ __  \n" +
                "| |\\/| |/ _` | '_ \\| |_) | | | | |/ _` | __/ _` | | |  _ / _ \\ '_ \\ \n" +
                "| |  | | (_| | |_) |  _ <  | |_| | (_| | || (_| | | |_| |  __/ | | |\n" +
                "|_|  |_|\\__,_| .__/|_| \\_\\ |____/ \\__,_|\\__\\__,_|  \\____|\\___|_| |_|\n" +
                "             |_|";
    }
    
    public String getOutput() {
        return this.output;
    }
    
    public String getClassName() {
        return this.className;
    }
    
    public long getRowCount() {
        return this.rowCount;
    }
    
    public int getPartitions() {
        return this.partitions;
    }
    
    public int getTasks() {
        return this.tasks;
    }
    
    public String getCompressionType() {
        return this.compressionType;
    }
    
    public int getVariableSize() {
        return this.variableSize;
    }
    
    public String getBanner() {
        return this.banner;
    }
    
    public int getShowRows() {
        return this.showRows;
    }
    
    public int getRangeInt() {
        return this.rangeInt;
    }
    
    private void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("parquet-generator", options);
    }
    
    public boolean getAffixRandom() {
        return this.affixRandom;
    }
    
    public Map<String, String> getDataSinkOptions() {
        return this.dataSinkOptions;
    }
    
    public String getOutputFileFormat() {
        return this.outputFileFormat;
    }
    
    private void showErrorAndExit(String str) {
        show_help();
        System.err.println("************ ERROR *******************");
        System.err.println(str);
        System.err.println("**************************************");
        System.exit(-1);
    }
    
    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        
        try {
            CommandLine cmd = parser.parse(options, args);
            
            if (cmd.hasOption("o")) {
                this.output = cmd.getOptionValue("o").trim();
            }
            
            if (cmd.hasOption("r")) {
                this.rowCount = Long.parseLong(cmd.getOptionValue("r").trim());
            }
            
            if (cmd.hasOption("f")) {
                this.outputFileFormat = cmd.getOptionValue("f").trim();
            }
            
            if (cmd.hasOption("C")) {
                this.compressionType = cmd.getOptionValue("C").trim();
            }
            
            if (cmd.hasOption("s")) {
                this.variableSize = Integer.parseInt(cmd.getOptionValue("s").trim());
            }
            
            if (cmd.hasOption("S")) {
                this.showRows = Integer.parseInt(cmd.getOptionValue("S").trim());
            }
            
            if (cmd.hasOption("p")) {
                this.partitions = Integer.parseInt(cmd.getOptionValue("p").trim());
            }
            
            if (cmd.hasOption("t")) {
                this.tasks = Integer.parseInt(cmd.getOptionValue("t").trim());
            }
            
            if (cmd.hasOption("R")) {
                this.rangeInt = Integer.parseInt(cmd.getOptionValue("R").trim());
            }
            
            if (cmd.hasOption("O")) {
                String[] vals = cmd.getOptionValue("O").split(",");
                if (vals.length != 2) {
                    System.err.println("Failed to parse " + cmd.getOptionValue("P"));
                    System.exit(-1);
                }
                /* otherwise we got stuff */
                dataSinkOptions.put(vals[0].trim(), vals[1].trim());
            }
    
            if (cmd.hasOption("c")) {
                this.concurrency = Integer.parseInt(cmd.getOptionValue("c").trim());
            }
            
        } catch (ParseException e) {
            showErrorAndExit("Failed to parse command line properties" + e);
        }
    }
    
    public Integer getConcurrency() {
        return concurrency;
    }
}
