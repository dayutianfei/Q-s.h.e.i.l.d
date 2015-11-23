package cn.dayutianfei.thrift.api.impl;

import org.apache.thrift.TException;

import cn.dayutianfei.thrift.api.DemoException;
import cn.dayutianfei.thrift.api.Status;
import cn.dayutianfei.thrift.api.ThriftService;
import cn.dayutianfei.thrift.api.thrift_protocolConstants;

public class DemoImpl implements ThriftService.Iface {

	@Override
	public String getName() throws TException {
		long currentTime = System.currentTimeMillis();
		return "dayu appears on "+currentTime;
	}

	@Override
	public String getVersion() throws TException {
		return thrift_protocolConstants.VERSION;
	}

	@Override
	public Status getStatus() throws TException {
		return Status.ALIVE;
	}

	@Override
	public String demoMethod(String arg) throws DemoException, TException {
		String input = arg;
		String output = "hello, there! what you input is "+input;
		return output;
	}

}
