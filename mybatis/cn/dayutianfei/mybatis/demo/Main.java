package cn.dayutianfei.mybatis.demo;

import cn.dayutianfei.mybatis.dao.AddressDao;

public class Main {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        AddressDao demo = new AddressDao();
        System.out.println(demo.getAddress(1).toString());
    }

    /**
     * 
     * INSERT INTO ADDRESSES (ADDR_ID,STREET,CITY,STATE,ZIP,COUNTRY) VALUES   (1,'4891 Pacific Hwy','San Diego','CA','92110','San Diego'),  (2,'2400 N Jefferson St','Perry','FL','32347','Taylor'),  (3,'710 N Cable Rd','Lima','OH','45825','Allen'),  (4,'5108 W Gore Blvd','Lawton','OK','32365','Comanche');
     * CREATE TABLE ADDRESSES  (   ADDR_ID INT(11) NOT NULL AUTO_INCREMENT,   STREET VARCHAR(50) NOT NULL,   CITY VARCHAR(50) NOT NULL,   STATE VARCHAR(50) NOT NULL,   ZIP VARCHAR(10) DEFAULT NULL,   COUNTRY VARCHAR(50) NOT NULL,   PRIMARY KEY (ADDR_ID) ) ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=LATIN1;
     * 
     * 
     * 
     */
}
