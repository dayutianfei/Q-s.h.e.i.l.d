package cn.dayutianfei.lusearch.server.loadmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.client.cache.ClientCache;
import cn.dayutianfei.loadserver.prototype.Field;


public class TypeJudgePolicy {

    private static Logger LOG = LoggerFactory.getLogger(TypeJudgePolicy.class);

    public List<String> judge(String dbName, String tableName, List<String> line) throws Exception {
        int index = 0;
        boolean notAllNUll = false;
        boolean judge_result = true;
        List<String> newLine = new ArrayList<String>();
        List<Field> tabFields = ClientCache.getTableInfo(dbName, tableName).getFields();
        for (Field field : tabFields) {
            if (field.isEnable()) {
                switch (field.getType()) {
                case TYPE_BOOLEAN:
                    boolean data01 = false;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else if ("true".equalsIgnoreCase(line.get(index).toString())) {
                            data01 = true;
                        }
                        else if ("false".equalsIgnoreCase(line.get(index).toString())) {
                            data01 = false;
                        }
                        else {
                            judge_result = false;
                            LOG.error("There is a type error about Boolean type on Location :" + index);
                        }
                        notAllNUll = true;
                        newLine.add(data01+"");
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about Boolean type on Location :" + index);
                    }
                    break;
                case TYPE_DECIMAL:
                    String data02 = null;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            String[] pars = field.getParameter().split(",");
                            if (pars.length == 2) {
                                int precision = Integer.parseInt(pars[0]);
                                int scope = Integer.parseInt(pars[1]);
                                data02 = vaildata(line.get(index).toString(), precision - scope, scope);
                                if (data02 == null) {
                                    judge_result = false;
                                    LOG.error("There is a error about DECIMAL type on Location : " + index);
                                    LOG.error("the error value ["
                                            + Integer.parseInt(line.get(index).toString()) + "]");

                                }
                            }
                        }
                        notAllNUll = true;
                        newLine.add(data02);
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about TINYINT type on Location  ::" + index);
                    }
                    break;
                case TYPE_TINYINT:
                    int data03;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            data03 = Integer.parseInt(line.get(index).toString());
                            if (data03 > 127 || data03 < -128) {
                                judge_result = false;
                                LOG.error("There is a range error about TINYINT type on Location" + index);
                                LOG.error("range is -128 to 127 but value [" + data03 + "]");
                            }
                        }
                        notAllNUll = true;
                        newLine.add(data03+"");
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about TINYINT type on Location  ::" + index);
                    }
                    break;
                case TYPE_SMALLINT:
                    int data04;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            data04 = Integer.parseInt(line.get(index).toString());
                            if (data04 > 32767 || data04 < -32768) {
                                judge_result = false;
                                LOG.error("There is a range error about SMALLINT type on Location" + index);
                                LOG.error("range is -32768 to 32767 but value ["
                                        + Integer.parseInt(line.get(index).toString()) + "]");
                            }
                        }
                        notAllNUll = true;
                        newLine.add(data04+"");
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about SMALLINT type on Location  ::" + index);
                    }
                    break;
                case TYPE_INT:
                case TYPE_INTEGER:
                    int data05;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            data05 = Integer.parseInt(line.get(index).toString());
                        }
                        notAllNUll = true;
                        newLine.add(data05+"");
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about INT type on Location  ::" + index);
                    }
                    break;
                case TYPE_BIGINT:
                    long data06;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            data06 = Long.parseLong(line.get(index).toString());
                        }
                        notAllNUll = true;
                        newLine.add(data06+"");
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about LONG type on Location  ::" + index);
                    }
                    break;
                case TYPE_DOUBLE:
                case TYPE_REAL:
                    double data07;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            if (isZero(line.get(index).toString())) {
                                data07 = 0.0;
                            }
                            else {
                                data07 = Double.parseDouble(line.get(index).toString());
                                if (Double.isInfinite(data07) || data07 == 0) {
                                    judge_result = false;
                                    LOG.error("There is a range error about DOUBLE or REAL type on Location"
                                            + index);
                                    LOG.error("range is 4.9E-324 to 1.7976931348623157E308 but value ["
                                            + Double.parseDouble(line.get(index).toString()) + "]");
                                }
                            }
                        }
                        notAllNUll = true;
                        newLine.add(data07+"");
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about DOUBLE type on Location  ::" + index);
                    }
                    break;
                case TYPE_FLOAT:
                    float data08;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            if (isZero(line.get(index).toString())) {
                                data08 = 0.0f;
                            }
                            else {
                                data08= Float.valueOf(line.get(index).toString());
                                if (Float.isInfinite(data08)) {
                                    judge_result = false;
                                    LOG.error("There is a range error about FLOAT type on Location" + index);
                                    LOG.error("range is 1.4E-45 to 3.4028235E38 but value ["
                                            + Double.parseDouble(line.get(index).toString()) + "]");
                                }
                            }
                            notAllNUll = true;
                            newLine.add(data08+"");
                        }
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about FLOAT type on Location  ::" + index);
                    }
                    break;
                case TYPE_CHAR:
                    String data09;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            data09 = line.get(index).toString();
                            char[] c = data09.toCharArray();
                            int length = Integer.parseInt(field.getParameter());
                            data09 = formatChar(c, length);
                            if (length == -1 && c.length > 255) {
                                judge_result = false;
                                LOG.error("There is a error about CHAR type on Location" + index);
                                LOG.error("range is 0 to " + 255 + " but length [" + c.length + "]");
                            }
                            else if (length != -1 && length < c.length) {
                                judge_result = false;
                                LOG.error("There is a error about CHAR type on Location" + index);
                                LOG.error("range is 0 to " + length + " but length [" + c.length + "]");
                            }
                        }
                        notAllNUll = true;
                        newLine.add(data09);
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about STRING type on Location  ::" + index);
                    }
                    break;
                case TYPE_VARCHAR:
                case TYPE_STRING:
                    String data10;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            data10 = line.get(index).toString();
                            int dataLength = data10.getBytes("utf8").length;
                            int length = Integer.parseInt(field.getParameter());
                            if (length == -1 && dataLength > 65355) {
                                judge_result = false;
                                LOG.error("There is a  error about VARCHAR type on Location" + index);
                                LOG.error("range is 0 to " + 65355 + " but length [" + dataLength + "]");
                            }
                            else if (length != -1 && length < dataLength) {
                                judge_result = false;
                                LOG.error("There is a  error about VARCHAR type on Location" + index);
                                LOG.error("range is 0 to " + length + " but length [" + dataLength + "]");
                            }
                        }
                        notAllNUll = true;
                        newLine.add(data10);
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about STRING type on Location  ::" + index);
                    }
                    break;
                case TYPE_BINARY:
                    byte[] data11;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            data11 = line.get(index).toString().getBytes("uft8");
                            int length = data11.length;
                            if (length > 2 * 1024 * 1024 * 1024) {
                                judge_result = false;
                                LOG.error("There is a  error about STRING type on Location" + index);
                                LOG.error("range is 0 to " + 2 * 1024 * 1024 * 1024 + " but length ["
                                        + length + "]");
                            }
                        }
                        notAllNUll = true;
                        newLine.add(data11+"");
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about BINARY type on Location  ::" + index);
                    }
                    break;
                case TYPE_TIMESTAMP:
                    String data12;
                    try {
                        if (line.get(index) == null) {
                            newLine.add(null);
                            break;
                        }
                        else {
                            data12 = isTimestamp(line.get(index).toString());
//                            data12 = line.get(index).toString();
                            if (data12 == null) {
                                judge_result = false;
                            }
                        }
                        notAllNUll = true;
                        newLine.add(data12);
                    }
                    catch (Exception e) {
                        judge_result = false;
                        LOG.error("There is a type error about BINARY type on Location  ::" + index);
                    }
                    break;
                default:
                    LOG.error("can not fount the type " + field.getType() + "  on Location  " + index);
                    judge_result = false;
                    break;
                }
                index++;
                if (!judge_result) {
                    List<List<String>> datas = new ArrayList<List<String>>();
                    datas.add(line);
                    //CommonUtil.writer(datas, dbName, tableName, "/temp/error");
                    throw new Exception("There is a type error on dbName [" + dbName + "] table[" + tableName
                            + "]");
                }
            }
            else {
                newLine.add(null);
            }
        }
        if (!notAllNUll) {
            List<List<String>> datas = new ArrayList<List<String>>();
            datas.add(line);
            //CommonUtil.writer(datas, dbName, tableName, "/temp/error");
            throw new Exception("The data in dbName [" + dbName + "] table[" + tableName + "] can't all null");
        }
        return newLine;
    }


    private String vaildata(String data, int precision, int scope) {
        Pattern p = null;
        int dataLength = data.length();
        for (int i = 0; i < dataLength - 1; i++) {
            if (data.startsWith("0")) {
                data = data.substring(1);
            }
            else if (data.startsWith(".")) {
                data = "0" + data;
                break;
            }
            else {
                break;
            }
        }
        if (precision == 0) {
            p = Pattern.compile("^((((\\-|\\+)?0{1}))|(((\\-|\\+)?0{1}))(\\.{1}[0-9]{1," + scope + "}))$");
        }
        else if (scope > 0 && data.contains(".")) {
            p = Pattern.compile("^((\\-|\\+)?[0-9]{0," + precision + "})(\\.{1}[0-9]{1," + scope + "})$");
        }
        else {
            p = Pattern.compile("^((\\-|\\+)?[0-9]{0," + precision + "})$");
        }
        if (p.matcher(data).matches()) {
            if (data.contains(".")) {
                int n = data.substring(data.indexOf(".") + 1).length();
                for (int i = 0; i < n; i++) {
                    if (data.endsWith("0")) {
                        data = data.substring(0, data.length() - 1);
                    }
                    else {
                        break;
                    }
                }
                if (data.endsWith(".")) {
                    data = data.substring(0, data.length() - 1);
                }
            }
        }
        else {
            data = null;
        }
        return data;
    }


    private String formatChar(char[] c, int length) {
        if (length == -1) {
            length = 255;
        }
        int i = 0;
        char[] result = new char[length];
        if (c.length <= length) {
            for (; i < c.length; i++) {
                result[i] = c[i];
            }
            for (; i < length; i++) {
                result[i] = ' ';
            }
        }
        else {
            for (; i < length; i++) {
                result[i] = c[i];
            }
        }
        return new String(result);
    }


    private boolean isZero(String data) {
        Pattern p = Pattern.compile("^(([0]{1})(.{1}[0]{0,300})|([0]{1}))$");
        return p.matcher(data).matches();
    }


    private String isTimestamp(String str) {
        if (str.contains(".")) {
            String s1 =
                    "^((([0-9]{4})-(([1-9])|(1[0-2]))-((0[1-9])|([1-2][0-9])|(3[0-1])))(((\\s)((0[0-9])|(1[0-9])|(2[0-3]))):([0-5][0-9]):([0-5][0-9]))(\\.{1}[0-9]{1,9}))$";
            Pattern p1 = Pattern.compile(s1);
            if (p1.matcher(str).matches()) {
                int size = 29 - str.length();
                if (size > 0) {
                    for (int i = 0; i < size; i++) {
                        str = str + "0";
                    }
                }
            }
            else {
                str = null;
            }

        }
        else if (str.contains(":")) {
            String s1 =
                    "^((([1][4-9][0-9]{2}|[2-9][0-9]{3})-((0[1-9])|(1[0-2]))-((0[1-9])|([1-2][0-9])|(3[0-1])))(((\\s)((0[0-9])|(1[0-9])|(2[0-3]))):([0-5][0-9]):([0-5][0-9])))$";
            Pattern p1 = Pattern.compile(s1);
            if (!p1.matcher(str).matches()) {
                str = null;
            }
        }
        else {
            String s1 =
                    "(([1][4-9][0-9]{2}|[2-9][0-9]{3})-((0[1-9])|(1[0-2]))-((0[1-9])|([1-2][0-9])|(3[0-1])))";
            Pattern p1 = Pattern.compile(s1);
            if (p1.matcher(str).matches()) {
                str = str + " 00:00:00";
            }
            else {
                str = null;
            }
        }
        return str;
    }
}
