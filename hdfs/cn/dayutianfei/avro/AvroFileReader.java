package cn.dayutianfei.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.google.common.collect.Lists;

public class AvroFileReader {
    
    public static void main(String[] args) throws IOException{
        Schema schema;
        String _schemaFilePath;
        _schemaFilePath = args[0];
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
        schema = new Schema.Parser().parse(schemaString);
        FileInputStream fis = new FileInputStream(new File("/tmp/avrofile/avro/2016-09-23-17-10-34-817_1474621834818"));
        decodeAvroFile(fis,schema);
//        List<List<Object>> out = deserialize(fis,schema);
//        for(List<Object> temp : out){
//            for(Object _temp: temp){
//                System.out.print(_temp.toString()+"\t");
//            }
//            System.out.println();
//        }
    }
    
    public static List<List<Object>> deserialize(FileInputStream body, Schema arraySchema) {
        GenericDatumReader<GenericArray<GenericRecord>> reader = new GenericDatumReader<GenericArray<GenericRecord>>(
                arraySchema);
        BinaryDecoder defaultDecoder = DecoderFactory.get().binaryDecoder(body,
                null);
        GenericArray<GenericRecord> array = new GenericData.Array<GenericRecord>(
                1000, arraySchema);
        List<List<Object>> ll = Collections.emptyList();
        try {
            reader.read(array, defaultDecoder);
        } catch (Exception e) {
            e.printStackTrace();
            return ll;
        }
        ll = Lists.newLinkedList();
        int fieldSize = arraySchema.getElementType().getFields().size();
        for (GenericRecord record : array) {
            List<Object> ol = Lists.newArrayListWithCapacity(fieldSize);
            for (int i = 0; i < fieldSize; ++i) {
                Object obj = record.get(i);
                ol.add(obj);
            }
            ll.add(ol);
        }
        return ll;
    }
    
    public static void decodeAvroFile(FileInputStream body, Schema arraySchema){
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                arraySchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(body,null);
        GenericRecord re = null;
        try {
            re = reader.read(null, decoder);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(re.toString());
    }
}
