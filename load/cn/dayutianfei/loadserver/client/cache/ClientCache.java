package cn.dayutianfei.loadserver.client.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.dayutianfei.loadserver.lib.serde.SchemaManager;
import cn.dayutianfei.loadserver.prototype.Field;
import cn.dayutianfei.loadserver.prototype.FieldTypeEnum;
import cn.dayutianfei.loadserver.prototype.TableInfo;


public class ClientCache {

    // node
    public static List<String> hostName = new ArrayList<String>();

    public static int HTTP_SOURCE_PORT = 5187;

    public static HashMap<String, TableInfo> tableCache = new HashMap<String, TableInfo>();


    public static void updateCache() {
        SchemaManager ss = new SchemaManager();
        ss.toString();
//        hostName.add("172.16.2.201");
//         hostName.add("172.16.2.202");
//         hostName.add("172.16.2.203");
         hostName.add("172.16.2.206");
        TableInfo test_info = new TableInfo();
        test_info.setDbName("test");
        test_info.setTableName("test1");
        ArrayList<Field> fields = new ArrayList<Field>();
        Field c1 = new Field();
        c1.setFieldName("id");
        c1.setType(FieldTypeEnum.TYPE_STRING);
        Field c2 = new Field();
        c2.setFieldName("name");
        c2.setType(FieldTypeEnum.TYPE_STRING);
        Field c3 = new Field();
        c3.setFieldName("address");
        c3.setType(FieldTypeEnum.TYPE_STRING);
        Field c4 = new Field();
        c4.setFieldName("address1");
        c4.setType(FieldTypeEnum.TYPE_STRING);
        Field c5 = new Field();
        c5.setFieldName("address2");
        c5.setType(FieldTypeEnum.TYPE_STRING);
        Field c6 = new Field();
        c6.setFieldName("address3");
        c6.setType(FieldTypeEnum.TYPE_STRING);
        Field c7 = new Field();
        c7.setFieldName("address4");
        c7.setType(FieldTypeEnum.TYPE_STRING);
        Field c8 = new Field();
        c8.setFieldName("address5");
        c8.setType(FieldTypeEnum.TYPE_STRING);
        Field c9 = new Field();
        c9.setFieldName("address6");
        c9.setType(FieldTypeEnum.TYPE_STRING);
        Field c10 = new Field();
        c10.setFieldName("address7");
        c10.setType(FieldTypeEnum.TYPE_STRING);
        Field c11 = new Field();
        c11.setFieldName("address8");
        c11.setType(FieldTypeEnum.TYPE_STRING);
        Field c12 = new Field();
        c12.setFieldName("address9");
        c12.setType(FieldTypeEnum.TYPE_STRING);
        Field c13 = new Field();
        c13.setFieldName("address10");
        c13.setType(FieldTypeEnum.TYPE_STRING);
        Field c14 = new Field();
        c14.setFieldName("address11");
        c14.setType(FieldTypeEnum.TYPE_STRING);
        Field c15 = new Field();
        c15.setFieldName("address12");
        c15.setType(FieldTypeEnum.TYPE_STRING);
        Field c16 = new Field();
        c16.setFieldName("address13");
        c16.setType(FieldTypeEnum.TYPE_STRING);
        Field c17 = new Field();
        c17.setFieldName("address14");
        c17.setType(FieldTypeEnum.TYPE_STRING);
        Field c18 = new Field();
        c18.setFieldName("address15");
        c18.setType(FieldTypeEnum.TYPE_STRING);
        Field c19 = new Field();
        c19.setFieldName("address16");
        c19.setType(FieldTypeEnum.TYPE_STRING);
        Field c20 = new Field();
        c20.setFieldName("address17");
        c20.setType(FieldTypeEnum.TYPE_STRING);
        fields.add(c1);        fields.add(c2);        fields.add(c3);        fields.add(c4);        fields.add(c5);
        fields.add(c6);        fields.add(c7);        fields.add(c8);        fields.add(c9);        fields.add(c10);
        fields.add(c11);        fields.add(c12);        fields.add(c13);        fields.add(c14);        fields.add(c15);
        fields.add(c16);        fields.add(c17);        fields.add(c18);        fields.add(c19);        fields.add(c20);
        test_info.setFields(fields);
        tableCache.put(test_info.getDbName() + test_info.getTableName(), test_info);
        System.out.println(test_info.toSchema());

        TableInfo normal_info = new TableInfo();
        normal_info.setDbName("test");
        normal_info.setTableName("normal");
        ArrayList<Field> field1 = new ArrayList<Field>();
        Field cc1 = new Field();
        cc1.setFieldName("id");
        cc1.setType(FieldTypeEnum.TYPE_STRING);
        Field cc2 = new Field();
        cc2.setFieldName("ms_byte");
        cc2.setType(FieldTypeEnum.TYPE_STRING);
        Field cc3 = new Field();
        cc3.setFieldName("ms_short");
        cc3.setType(FieldTypeEnum.TYPE_STRING);
        Field cc4 = new Field();
        cc4.setFieldName("ms_int");
        cc4.setType(FieldTypeEnum.TYPE_STRING);
        Field cc5 = new Field();
        cc5.setFieldName("ms_long");
        cc5.setType(FieldTypeEnum.TYPE_STRING);
        Field cc6 = new Field();
        cc6.setFieldName("m_float");
        cc6.setType(FieldTypeEnum.TYPE_STRING);
        Field cc7 = new Field();
        cc7.setFieldName("m_double");
        cc7.setType(FieldTypeEnum.TYPE_STRING);
        Field cc8 = new Field();
        cc8.setFieldName("allchar");
        cc8.setType(FieldTypeEnum.TYPE_STRING);
        Field cc9 = new Field();
        cc9.setFieldName("allchar_no");
        cc9.setType(FieldTypeEnum.TYPE_STRING);
        Field cc10 = new Field();
        cc10.setFieldName("allvarchar");
        cc10.setType(FieldTypeEnum.TYPE_STRING);
        Field cc11 = new Field();
        cc11.setFieldName("allvarchar_no");
        cc11.setType(FieldTypeEnum.TYPE_STRING);
        Field cc12 = new Field();
        cc12.setFieldName("allvarchar_url");
        cc12.setType(FieldTypeEnum.TYPE_STRING);
        Field cc13 = new Field();
        cc13.setFieldName("allchar_url");
        cc13.setType(FieldTypeEnum.TYPE_STRING);
        Field cc14 = new Field();
        cc14.setFieldName("ipv4");
        cc14.setType(FieldTypeEnum.TYPE_STRING);
        Field cc15 = new Field();
        cc15.setFieldName("ipv6");
        cc15.setType(FieldTypeEnum.TYPE_STRING);
        Field cc16 = new Field();
        cc16.setFieldName("content");
        cc16.setType(FieldTypeEnum.TYPE_STRING);
        Field cc17 = new Field();
        cc17.setFieldName("d_binary");
        cc17.setType(FieldTypeEnum.TYPE_BINARY);
        Field cc18 = new Field();
        cc18.setFieldName("d_lstore");
        cc18.setType(FieldTypeEnum.TYPE_STRING);
        Field cc19 = new Field();
        cc19.setFieldName("d_store");
        cc19.setType(FieldTypeEnum.TYPE_STRING);
        Field cc20 = new Field();
        cc20.setFieldName("time");
        cc20.setEnable(true);
        cc20.setType(FieldTypeEnum.TYPE_STRING);
        Field cc21 = new Field();
        cc21.setFieldName("times");
        cc21.setType(FieldTypeEnum.TYPE_STRING);
        field1.add(cc1);        field1.add(cc2);        field1.add(cc3);        field1.add(cc4);        field1.add(cc5);
        field1.add(cc6);        field1.add(cc7);        field1.add(cc8);        field1.add(cc9);        field1.add(cc10);
        field1.add(cc11);        field1.add(cc12);        field1.add(cc13);        field1.add(cc14);        field1.add(cc15);
        field1.add(cc16);        field1.add(cc17);        field1.add(cc18);        field1.add(cc19);        field1.add(cc20);
        field1.add(cc21);
        normal_info.setFields(field1);
        System.out.println(normal_info.toSchema());
        tableCache.put(normal_info.getDbName() + normal_info.getTableName(), normal_info);

        TableInfo test_tbl_info = new TableInfo();
        test_tbl_info.setDbName("test");
        test_tbl_info.setTableName("test_info");
        ArrayList<Field> field2 = new ArrayList<Field>();
        Field dd1 = new Field();
        dd1.setFieldName("dd1");
        dd1.setEnable(true);
        dd1.setType(FieldTypeEnum.TYPE_INT);
        Field dd2 = new Field();
        dd2.setFieldName("dd2");
        dd2.setEnable(true);
        dd2.setType(FieldTypeEnum.TYPE_INT);
        Field dd3 = new Field();
        dd3.setFieldName("dd3");
        dd3.setEnable(true);
        dd3.setType(FieldTypeEnum.TYPE_INT);
        Field dd4 = new Field();
        dd4.setFieldName("dd4");
        dd4.setEnable(true);
        dd4.setType(FieldTypeEnum.TYPE_INT);
        Field dd5 = new Field();
        dd5.setFieldName("dd5");
        dd5.setEnable(true);
        dd5.setType(FieldTypeEnum.TYPE_INT);
        Field dd6 = new Field();
        dd6.setFieldName("dd6");
        dd6.setEnable(true);
        dd6.setType(FieldTypeEnum.TYPE_DOUBLE);
        Field dd7 = new Field();
        dd7.setFieldName("dd7");
        dd7.setEnable(true);
        dd7.setType(FieldTypeEnum.TYPE_DOUBLE);
        Field dd8 = new Field();
        dd8.setFieldName("dd8");
        dd8.setEnable(true);
        dd8.setType(FieldTypeEnum.TYPE_DOUBLE);
        Field dd9 = new Field();
        dd9.setFieldName("dd9");
        dd9.setEnable(true);
        dd9.setType(FieldTypeEnum.TYPE_CHAR);
        dd9.setParameter("2");
        Field dd10 = new Field();
        dd10.setFieldName("dd10");
        dd10.setEnable(true);
        dd10.setType(FieldTypeEnum.TYPE_CHAR);
        dd10.setParameter("2");
        Field dd11 = new Field();
        dd11.setFieldName("dd11");
        dd11.setEnable(true);
        dd11.setType(FieldTypeEnum.TYPE_TIMESTAMP);
        Field dd12 = new Field();
        dd12.setFieldName("dd12");
        dd12.setEnable(true);
        dd12.setType(FieldTypeEnum.TYPE_TIMESTAMP);
        Field dd13 = new Field();
        dd13.setFieldName("dd13");
        dd13.setEnable(true);
        dd13.setType(FieldTypeEnum.TYPE_TIMESTAMP);
        Field dd14 = new Field();
        dd14.setFieldName("dd14");
        dd14.setEnable(true);
        dd14.setType(FieldTypeEnum.TYPE_VARCHAR);
        dd14.setParameter("50");
        Field dd15 = new Field();
        dd15.setFieldName("dd15");
        dd15.setEnable(true);
        dd15.setType(FieldTypeEnum.TYPE_VARCHAR);
        dd15.setParameter("20");
        Field dd16 = new Field();
        dd16.setFieldName("dd16");
        dd16.setEnable(true);
        dd16.setType(FieldTypeEnum.TYPE_VARCHAR);
        dd16.setParameter("100");
        field2.add(dd1);        field2.add(dd2);        field2.add(dd3);        field2.add(dd4);        field2.add(dd5);
        field2.add(dd6);        field2.add(dd7);        field2.add(dd8);        field2.add(dd9);        field2.add(dd10);
        field2.add(dd11);        field2.add(dd12);        field2.add(dd13);      field2.add(dd14);        field2.add(dd15);
        field2.add(dd16);
        test_tbl_info.setFields(field2);
        System.out.println(test_tbl_info.toSchema());
        tableCache.put(test_tbl_info.getDbName() + test_tbl_info.getTableName(), test_tbl_info);
    }


    public static TableInfo getTableInfo(String db, String tblName) {
        if (tableCache.containsKey(db + tblName)) {
            return tableCache.get(db + tblName);
        }
        else {
            return null;
        }
    }
}
