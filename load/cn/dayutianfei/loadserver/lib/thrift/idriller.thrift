/**
 *
 * Based by flume.thrift which in flume-ng sdk package.
 *
 **
 *
 * Modified by zhenyu.wang for idriller-8.9's load module, a loader system.
 * 1. Add a new RPC message to get schema string of specific table.
 * 2. Add a new RPC method to get the server's state
 * 
 */
 
namespace java cn.dayutianfei.loadserver.lib.thrift.protocol

struct ThriftFlumeEvent {
  1: required map <string, string> headers,
  2: required binary body,
}

enum Status {
  OK,
  FAILED,
  ERROR,
  UNKNOWN
}

service ThriftSourceProtocol {
  string getSchema(1: string dbName, 2: string tblName),
  string getQ(),
  Status append(1: ThriftFlumeEvent event),
  Status appendBatch(1: list<ThriftFlumeEvent> events),
}
