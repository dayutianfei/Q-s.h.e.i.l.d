/**
 * IK 中文分词  版本 5.0.1
 * IK Analyzer release 5.0.1
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * 源代码由林良益(linliangyi2005@gmail.com)提供
 * 版权声明 2012，乌龙茶工作室
 * provided by Linliangyi and copyright 2012 by Oolong studio
 * 
 * 
 */
package cn.dayutianfei.lucene.analyzer.sci;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer5x;
import org.wltea.analyzer.dic.Dictionary;
/**
 * 使用IKAnalyzer进行分词的演示
 * 2012-10-22
 *
 */
public class SciIKAnalzyerDemo extends Thread {
	  
	@SuppressWarnings("resource")
    public static void main(String[] args) throws InterruptedException, IOException{
		String path1 = "lucene/dic.xml";
		//String path0 = "IKAnalyzer.cfg.xml";
//		
//		
		Dictionary.initDic(path1);//加载词典
		
		//IKAnalyzer（是否智能分词）
		Analyzer analyzer = new IKAnalyzer5x(true, path1);
//		Analyzer analyzer = new StandardAnalyzer();
//		获取Lucene的TokenStream对象
	    TokenStream ts = null;
	    //从以下目录中读取内容  	    
	    String path="/temp/lucene/data";
	   	//String tmp = "C:\\Users\\Administrator\\Desktop\\yue.txt";    
	    String[] cistring= readtxts(path);
	    //String cistring = "         http";
	    
	     	//FileOutputStream	writerStream = new FileOutputStream(tmp);
			//BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(writerStream, "UTF-8"));
	
			for (int i=0;i<cistring.length;i++){
				ts = analyzer.tokenStream("myfield", new StringReader(cistring[i]));//完成初始化
				OffsetAttribute  offset = ts.addAttribute(OffsetAttribute.class);			
				CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
				TypeAttribute type = ts.addAttribute(TypeAttribute.class);
				ts.reset(); 
				while (ts.incrementToken()) {
				System.out.println(offset.startOffset() + " - " + offset.endOffset() + " : " + term.toString() + " | " + type.type());
//				System.out.print( term.toString() +"\r\n");
//				writer.write(term.toString()+"\r\n");	
				}
				//关闭TokenStream（关闭StringReader）
				ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
				ts.close();
			}
			//writer.close();
	}

	public static String[] readtxts(String path){
		File file = new File(path);
		File[] array = file.listFiles(); 
		
		String[] text=new String[array.length];
		for(int i=0;i<text.length;i++){
			text[i]=readtxt(array[i].toString());
		}
		return text;
	}
	public static String readtxt(String path){
		String text="";
		try{
			System.out.println(path);
			File file=new File(path);
			
			if (file.isFile() && file.exists()) {   
				InputStreamReader read = new InputStreamReader(new FileInputStream(file),"utf-8");   
				BufferedReader bufferedReader = new BufferedReader(read);   
				String lineTXT = null;   
				while ((lineTXT = bufferedReader.readLine()) != null) {
					//System.out.println(lineTXT.toString().trim());   
					text=text+lineTXT.toString().trim();
					//System.out.println(lineTXT.toString().trim());
			}   
			read.close();   
			}else{   
				System.out.println("找不到指定的文件！");   
			}
		
			}
		catch (Exception e) {   
			System.out.println("读取文件内容操作出错");   
			e.printStackTrace();   
		}   
		return text;
	}

}
	
