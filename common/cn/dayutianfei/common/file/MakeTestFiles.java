package cn.dayutianfei.common.file;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.lang.math.RandomUtils;

public class MakeTestFiles {
    public static void main(String[] args){
        try {
            String seeds = "afljsabadwoieurowj12435u080532523j";
            // 打开一个随机访问文件流，按读写方式
            RandomAccessFile randomFile = new RandomAccessFile("/tmp/test1.csv", "rw");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            // 将写文件指针移到文件尾。
            randomFile.seek(fileLength);
            for(int i=0;i<10000000;i++){
                String content = "t"+i;
                for(int column=0;column<20;column++){
                    int start = RandomUtils.nextInt(seeds.length()-1);
                    System.out.println(start);
                    String temp = seeds.substring(start);
                    content = content+","+temp;
                }
                content+="\n";
                randomFile.writeBytes(content);
            }
            randomFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
