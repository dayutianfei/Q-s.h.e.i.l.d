package test;

import java.util.ArrayList;
import java.util.List;

public class TestTran {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        List<Object> test = new ArrayList<Object>();
        test.add("test");
        test.add("1");
        test.add("1345677");
        test.add("1");
        test.add("test");
        test.add("test");
        test.add("1345677");
        test.add("test");
        List<String> test1 = new ArrayList<String>();
        test1.add("test");
        test1.add("1");
        test1.add("1345677");
        test1.add("1");
        test1.add("test");
        test1.add("test");
        test1.add("1345677");
        test1.add("test");
        int times = 10000000;
        long start = System.currentTimeMillis();
        for(int i =0;i< times; i++){
            objectList2StringArray(test, test.size());
        }
        System.out.println("cost : "+(System.currentTimeMillis() - start)+" ms");
        
        long start1 = System.currentTimeMillis();
        for(int i =0;i< times; i++){
            objectList2StringArray(test1, test1.size(), true);
        }
        System.out.println("cost : "+(System.currentTimeMillis() - start1)+" ms");
    }

    public static String[] objectList2StringArray(List<Object> list,int size){
//        List<Field> fields=tableInfo.getFields();
        String[] record=new String[size];
        for (int i = 0; i < record.length; i++) {
            if (list.get(i) == null) {
                record[i]="\\N";
            }else{
                record[i]=String.valueOf(list.get(i));
//                record[i]=list.get(i).toString();
            }
        }
        return record;
    }
    public static String[] objectList2StringArray(List<String> list,int size, boolean a){
//      List<Field> fields=tableInfo.getFields();
      String[] record=new String[size];
      for (int i = 0; i < record.length; i++) {
          if (list.get(i) == null) {
              record[i]="\\N";
          }else{
              record[i]=list.get(i);
          }
      }
      return record;
  }
}
