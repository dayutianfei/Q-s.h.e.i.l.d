package cn.dayutianfei.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.avro.Schema.Type.*;


public class AvroFileGen {

    private static final Logger LOG = LoggerFactory.getLogger(AvroFileGen.class);

    private Schema schema;
    private GenericDatumWriter<GenericRecord> writer;
    private DataFileWriter<GenericRecord> fileWriter;

    private String _schemaFilePath;
    private String _csvFileDir;
    private String destDir;
    private SimpleDateFormat dateFormat;

    public AvroFileGen(String schemaFilePath, String csvFileDir, String outAvroFileDir) throws IOException {
        _schemaFilePath = schemaFilePath;
        _csvFileDir = csvFileDir;
        String schemaString = "";
        BufferedReader reader = null;
        try{
            reader = new BufferedReader(new FileReader(_schemaFilePath));
            String tempString = "";
            while ((tempString = reader.readLine()) != null) {
                if(tempString.trim().length()==0){
                    continue;
                }
                schemaString +=tempString;
            }
        }catch(Exception e){
            System.exit(0);
        }
        System.out.println(schemaString);
        schema = new Schema.Parser().parse(schemaString);
        System.out.println(schema.getFields().size());
        destDir = outAvroFileDir;
        writer = new GenericDatumWriter<GenericRecord>(schema);
        fileWriter = new DataFileWriter<GenericRecord>(writer);
        dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-S");
    }


    public void run() throws IOException {
        File csvDir = new File(_csvFileDir);
        File[] csvFiles = csvDir.listFiles();
        if(csvFiles.length > 0){
            for (File _csv : csvFiles) {
                doTransfer(_csv,",");
            }
        }
    }


    public void doTransfer(File csv, String splitChar){
        String filename = dateFormat.format(new Date())+"_"+System.currentTimeMillis();
        String tmpFilename = filename.concat(".tmp");
        File tmpFile = new File(destDir, tmpFilename);
        LOG.info(String.format("Temp file: %s.", tmpFile.toString()));
        try {
            fileWriter.create(schema, tmpFile);
        }
        catch (IOException e1) {
            e1.printStackTrace();
        }
        BufferedReader reader = null;
        try{
            reader = new BufferedReader(new FileReader(csv));
            String tempString = "";
            while ((tempString = reader.readLine()) != null) {
                if(tempString.trim().length()==0){
                    continue;
                }
                System.out.println(tempString);
                GenericRecord avroRecord = new GenericData.Record(schema);
                final String notSupportedError = "Avro type %s is not supported.";
                try {
                    System.out.println(schema.getFields().size());
                    for (Schema.Field field : schema.getFields()) {
                        String fieldStr = tempString.split(splitChar)[field.pos()];
                        System.out.println(fieldStr + " " + field.pos());
                        switch (field.schema().getType()) {
                        case STRING:
                            avroRecord.put(field.name(), fieldStr);
                            break;
                        case NULL:
                            throw new Exception(String.format(notSupportedError, NULL.getName()));
                        case BYTES:
                            throw new Exception(String.format(notSupportedError, BYTES.getName()));
                        case ENUM:
                            throw new Exception(String.format(notSupportedError, ENUM.getName()));
                        case ARRAY:
                            throw new Exception(String.format(notSupportedError, ARRAY.getName()));
                        case MAP:
                            throw new Exception(String.format(notSupportedError, MAP.getName()));
                        case UNION:
                            if (field.schema().getTypes().size() != 2 && !field.schema().getTypes().contains(NULL))
                                throw new Exception(
                                    String.format("Union form is not is incorrect: %s.", field.schema().toString()));
                            else {
                                if ("".equals(fieldStr)) {
                                    avroRecord.put(field.name(), null);
                                    break;
                                }
                            }
                            for (Schema unionMember : field.schema().getTypes()) {
                                parsePrimary(avroRecord, field, unionMember, fieldStr);
                            }
                            break;
                        case FIXED:
                            throw new Exception(String.format(notSupportedError, FIXED.getName()));
                        case RECORD:
                            throw new Exception(String.format(notSupportedError, RECORD.getName()));
                        default:
                            parsePrimary(avroRecord, field, field.schema(), fieldStr);
                        }
                    }
                }
                catch (Exception e) {
                }
            }
        }catch(Exception e){
        }finally{
            try {
                if(null != fileWriter){
                    fileWriter.close();
                }
                if(null != reader){
                    reader.close();
                }
                
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        File destFile = new File(destDir, filename);
        if (tmpFile.renameTo(destFile))
            tmpFile.delete();
        LOG.info(String.format("File %s generated.", destFile.toString()));
    }


    private void parsePrimary(GenericRecord avroRecord, Schema.Field field, Schema schema, String fieldStr) {
        switch (schema.getType()) {
        case INT:
            try {
                avroRecord.put(field.name(), Integer.parseInt(fieldStr));
                return;
            }
            catch (NumberFormatException e) {
                throw e;
            }
        case LONG:
            try {
                avroRecord.put(field.name(), Long.parseLong(fieldStr));
                return;
            }
            catch (NumberFormatException e) {
                throw e;
            }
        case FLOAT:
            try {
                avroRecord.put(field.name(), Float.parseFloat(fieldStr));
                return;
            }
            catch (NumberFormatException e) {
                throw e;
            }
        case DOUBLE:
            try {
                avroRecord.put(field.name(), Double.parseDouble(fieldStr));
                return;
            }
            catch (NumberFormatException e) {
                throw e;
            }
        case BOOLEAN:
            try {
                avroRecord.put(field.name(), Boolean.parseBoolean(fieldStr));
            }
            catch (NumberFormatException e) {
                throw e;
            }
        default:
            break;
        }
    }


    public static void main(String[] args) throws IOException {
        String schemaFilePath=args[0];
        String csvFileDir=args[1];
        String outAvroFileDir = args[2];
        System.out.println(schemaFilePath+","+csvFileDir+","+outAvroFileDir);
        new AvroFileGen(schemaFilePath,csvFileDir,outAvroFileDir).run();
    }
}
