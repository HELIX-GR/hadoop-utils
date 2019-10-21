package gr.helix.hadoop_utils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extract the contents of a <tt>TFile</tt>-encoded logfile of a YARN application.
 * <p>
 * Note: YARN stores application logs under hdfs:///logs/{user}/logs/{applicationId}.
 * 
 * @see https://hadoop.apache.org/docs/r2.9.0/api/org/apache/hadoop/io/file/tfile/TFile.html
 */
public class TFileLogExtractor
{
    private static final Logger logger = LoggerFactory.getLogger(TFileLogExtractor.class);
    
    private final Configuration conf;
    
    private final Path inputPath;
    
    public TFileLogExtractor(Configuration conf, Path inputPath)
    {
        this.conf = conf;
        this.inputPath = inputPath;
    }
    
    public void extractToDirectory(Path outputDir) 
        throws IOException
    {
        try (
            final FileSystem inputFs = FileSystem.get(conf);
            final FileSystem outputFs = FileSystem.getLocal(conf)) 
        {
            logger.info("Using input filesystem at {}", inputFs.getUri());
            logger.info("Using output filesystem at {}", outputFs.getUri());
            
            ContentSummary contentSummary = inputFs.getContentSummary(inputPath);
            final long inputSize = contentSummary.getLength();
            logger.info("Extracting from {}: size is {} bytes", inputPath, inputSize);
            
            try (
                final FSDataInputStream in = inputFs.open(inputPath); 
                final TFile.Reader inputReader = new TFile.Reader(in, inputSize, conf)) 
            {
                extractToDirectory(inputReader, outputFs, outputDir);
            }
        }
    }
    
    private void extractToDirectory(TFile.Reader inputReader, FileSystem outputFs, Path outputDir)
        throws IOException
    {
        TFile.Reader.Scanner scanner = inputReader.createScanner();
        
        String key, value;
        
        for (; !scanner.atEnd(); scanner.advance()) {
            final TFile.Reader.Scanner.Entry entry = scanner.entry();
            
            try (DataInputStream keyIn = entry.getKeyStream()) {
                key = keyIn.readUTF();
            }
            
            // Extract value depending on the type of entry.
            // Based on org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader
            
            try (DataInputStream valueIn = entry.getValueStream()) {
                if (!key.startsWith("container_")) {
                    value = valueIn.readUTF();
                    logger.info("{}: {}={}", inputPath, key, value);
                } else {
                    Path containerLogsOutputDir = new Path(outputDir, new Path(key));
                    outputFs.mkdirs(containerLogsOutputDir);
                    logger.info("Extracting log files under {}", containerLogsOutputDir);
                    
                    String logType = null; 
                    boolean eod = false;
                    while (true) {
                        try {
                            logType = valueIn.readUTF();
                        } catch (EOFException x) {
                            eod = true;
                        }
                        if (eod)
                            break;
                        // Copy log contents into a separate file
                        final long len = Long.parseLong(valueIn.readUTF());
                        final Path outputPath = new Path(containerLogsOutputDir, new Path(logType));
                        try (BoundedInputStream valueIn1 = new BoundedInputStream(valueIn, len);
                            FSDataOutputStream out1 = outputFs.create(outputPath, true)) 
                        {
                            valueIn1.setPropagateClose(false); // do not close valueIn!
                            IOUtils.copyLarge(valueIn1, out1);
                        }
                    }
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception
    {
        if (args.length < 2) {
            System.err.printf("java %s <logfile> <output-dir>%n", TFileLogExtractor.class.getName());
            System.exit(-1);
        }
        
        final Configuration conf = new YarnConfiguration();
        final Path inputPath = new Path(args[0]);
        final Path outputDir = new Path(args[1]);
          
        final TFileLogExtractor p = new TFileLogExtractor(conf, inputPath);
        p.extractToDirectory(outputDir);
    }
    
}    
