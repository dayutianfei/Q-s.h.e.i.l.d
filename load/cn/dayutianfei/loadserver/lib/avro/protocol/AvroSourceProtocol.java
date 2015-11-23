/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package cn.dayutianfei.loadserver.lib.avro.protocol;

@SuppressWarnings("all")
/** *
 * Based by flume.avdl which in flume-ng sdk package.
 *
 **
 *
 * Modified by zhenyu.wang for idriller-8.9's load moudle, a loader system.
 * 1. Add a new RPC message to get schema string of specific table.
 * 2. Add a new RPC method to get the server's state
 * 
 * usage:
 * you need avro-1.7.7.jar， avro-tools-1.7.7.jar，core-asl，mapper-asl
 * java -jar avro-tools-1.7.7.jar compile schema user.avsc java */
@org.apache.avro.specific.AvroGenerated
public interface AvroSourceProtocol {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"AvroSourceProtocol\",\"namespace\":\"cn.dayutianfei.loadserver.lib.avro.protocol\",\"doc\":\"*\\r\\n * Based by flume.avdl which in flume-ng sdk package.\\r\\n *\\r\\n **\\r\\n *\\r\\n * Modified by zhenyu.wang for idriller-8.9's load moudle, a loader system.\\r\\n * 1. Add a new RPC message to get schema string of specific table.\\r\\n * 2. Add a new RPC method to get the server's state\\r\\n * \\r\\n * usage:\\r\\n * you need avro-1.7.7.jar， avro-tools-1.7.7.jar，core-asl，mapper-asl\\r\\n * java -jar avro-tools-1.7.7.jar compile schema user.avsc java\",\"types\":[{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"OK\",\"FAILED\",\"UNKNOWN\"]},{\"type\":\"record\",\"name\":\"AvroFlumeEvent\",\"fields\":[{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"body\",\"type\":\"bytes\"}]}],\"messages\":{\"getSchema\":{\"request\":[{\"name\":\"dbName\",\"type\":\"string\"},{\"name\":\"tblName\",\"type\":\"string\"}],\"response\":\"string\"},\"getQ\":{\"request\":[],\"response\":\"string\"},\"append\":{\"request\":[{\"name\":\"event\",\"type\":\"AvroFlumeEvent\"}],\"response\":\"Status\"},\"appendBatch\":{\"request\":[{\"name\":\"events\",\"type\":{\"type\":\"array\",\"items\":\"AvroFlumeEvent\"}}],\"response\":\"Status\"}}}");
  java.lang.CharSequence getSchema(java.lang.CharSequence dbName, java.lang.CharSequence tblName) throws org.apache.avro.AvroRemoteException;
  java.lang.CharSequence getQ() throws org.apache.avro.AvroRemoteException;
  cn.dayutianfei.loadserver.lib.avro.protocol.Status append(cn.dayutianfei.loadserver.lib.avro.protocol.AvroFlumeEvent event) throws org.apache.avro.AvroRemoteException;
  cn.dayutianfei.loadserver.lib.avro.protocol.Status appendBatch(java.util.List<cn.dayutianfei.loadserver.lib.avro.protocol.AvroFlumeEvent> events) throws org.apache.avro.AvroRemoteException;

  @SuppressWarnings("all")
  /** *
 * Based by flume.avdl which in flume-ng sdk package.
 *
 **
 *
 * Modified by zhenyu.wang for idriller-8.9's load moudle, a loader system.
 * 1. Add a new RPC message to get schema string of specific table.
 * 2. Add a new RPC method to get the server's state
 * 
 * usage:
 * you need avro-1.7.7.jar， avro-tools-1.7.7.jar，core-asl，mapper-asl
 * java -jar avro-tools-1.7.7.jar compile schema user.avsc java */
  public interface Callback extends AvroSourceProtocol {
    public static final org.apache.avro.Protocol PROTOCOL = cn.dayutianfei.loadserver.lib.avro.protocol.AvroSourceProtocol.PROTOCOL;
    void getSchema(java.lang.CharSequence dbName, java.lang.CharSequence tblName, org.apache.avro.ipc.Callback<java.lang.CharSequence> callback) throws java.io.IOException;
    void getQ(org.apache.avro.ipc.Callback<java.lang.CharSequence> callback) throws java.io.IOException;
    void append(cn.dayutianfei.loadserver.lib.avro.protocol.AvroFlumeEvent event, org.apache.avro.ipc.Callback<cn.dayutianfei.loadserver.lib.avro.protocol.Status> callback) throws java.io.IOException;
    void appendBatch(java.util.List<cn.dayutianfei.loadserver.lib.avro.protocol.AvroFlumeEvent> events, org.apache.avro.ipc.Callback<cn.dayutianfei.loadserver.lib.avro.protocol.Status> callback) throws java.io.IOException;
  }
}