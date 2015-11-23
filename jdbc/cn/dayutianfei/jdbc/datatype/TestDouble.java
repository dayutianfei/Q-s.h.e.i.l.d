package cn.dayutianfei.jdbc.datatype;

import java.math.BigDecimal;

public class TestDouble {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
//        String d = "1212238.99999888899";//截断1212238.999998889
        String d = "1212238.99999888899";
        Double dd_new = new Double(d);
        double ddd_new = Double.parseDouble(d);
        double com= 1234128.99999999999;
        BigDecimal xx = new BigDecimal(d);
        double xx_new = xx.precision();
        if(com > ddd_new){
            System.out.println("true"+xx_new);
        }
        System.out.println("double value from string is " + xx);
        System.out.println("double value from string is " + dd_new);
    }

}
