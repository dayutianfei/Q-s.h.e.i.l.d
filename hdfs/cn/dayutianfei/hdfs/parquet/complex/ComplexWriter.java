package cn.dayutianfei.hdfs.parquet.complex;

import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import parquet.hadoop.api.WriteSupport;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

public class ComplexWriter {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Path path = new Path("/temp/parquet/text.parquet");
		/*
		 create table fuza(a int, b array<string>, c map<string,string>, d 
		 struct<number:int,score:float>) stored as parquet;
		
		message spark_schema {
		  optional int32 a;
		  optional group b (LIST) {
		    repeated group bag {
		      optional binary array_element (UTF8);
		    }
		  }
		  optional group c (MAP) {
		    repeated group map (MAP_KEY_VALUE) {
		      required binary key (UTF8);
		      optional binary value (UTF8);
		    }
		  }
		  optional group d {
		    optional int32 number;
		    optional float score;
		  }
		}
		*/
		List<Type> typeList = Lists.newArrayList();
		String columnA = "a";
		String columnB = "b";
		String columnC = "c";
		String columnD = "d";
		// column a:int
		typeList.add(new PrimitiveType(Repetition.OPTIONAL,
				PrimitiveTypeName.INT32, columnA));
		// column b:list
		typeList.add(new GroupType(Repetition.OPTIONAL, columnB, OriginalType.LIST,
				new GroupType(Repetition.REPEATED, "bag", new PrimitiveType(
						Repetition.OPTIONAL, PrimitiveTypeName.BINARY,
						"array_element", OriginalType.UTF8))));
		// column c:map
		List<Type> list = Lists.newArrayList();
		list.add(new PrimitiveType(Repetition.REQUIRED,
				PrimitiveTypeName.BINARY, "key", OriginalType.UTF8));
		list.add(new PrimitiveType(Repetition.OPTIONAL,
				PrimitiveTypeName.BINARY, "value", OriginalType.UTF8));
		typeList.add(new GroupType(Repetition.OPTIONAL, columnC, OriginalType.MAP,
				new GroupType(Repetition.REPEATED, "map",
						OriginalType.MAP_KEY_VALUE, list)));
		// column d:struct
		List<Type> list2 = Lists.newArrayList();
		list2.add(new PrimitiveType(Repetition.OPTIONAL,
				PrimitiveTypeName.INT32, "number"));
		list2.add(new PrimitiveType(Repetition.OPTIONAL,
				PrimitiveTypeName.FLOAT, "score"));
		typeList.add(new GroupType(
				Repetition.OPTIONAL, columnD, list2));
		MessageType schema = new MessageType("hive_schema", typeList);

		WriteSupport<String[]> writeSupport = (WriteSupport<String[]>) new CWriteSupport(
				schema);
		Configuration conf = new Configuration();
		CParquetWriter writer = new CParquetWriter(path, schema, writeSupport,
				conf);
		Random random = new Random(System.nanoTime());
		for (int i = 0; i < 100; ++i) {
			String[] array = new String[4];
			array[0] = i + "";
			array[1] = String.format(
					"array_%s_1,array_%s_2,array_%s_3,array_%s_4", i, i, i, i);
			array[2] = String.format("key_1_%s:value_1_%s,key_2_%s:value_2_%s",
					i, i, i, i);
			array[3] = String.format("%s,%s", i, random.nextFloat());
			writer.write(array);
		}
		System.out.println("ok");
		writer.close();
	}
}
