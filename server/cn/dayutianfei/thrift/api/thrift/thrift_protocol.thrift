#!/usr/local/bin/thrift --gen java --gen py --gen php --gen perl --gen rb --gen csharp --gen cpp

namespace java cn.dayutianfei.thrift.api
namespace cpp cn.dayutianfei.thrift
namespace csharp Dayutianfei.Thrift
namespace py thrift
namespace php thrift
namespace perl Thrift
namespace rb DayutianfeiThrift

const string VERSION = "0.1"

struct DemoStruct {
	1: required double latitude,
	2: required double longitude,
	3: required string name,
	4: optional string country
}

exception DemoException {
    1: required string why
}

/**
 * Common status reporting mechanism across all services
 */
enum Status {
	DEAD = 0,
	STARTING = 1,
	ALIVE = 2,
	STOPPING = 3,
	STOPPED = 4,
	WARNING = 5,
}

/**
 * Thrift service
 * Demo
 * Add the method you need here, and then do the command to create the api.
 */
service ThriftService {

	/**
	 * Returns a descriptive name of the service
	 */
	string getName(),

	/**
	 * Returns the version of the service
	 */
	string getVersion(),

	/**
	 * Gets the status of this service
	 */
	Status getStatus(),

	/**
	 * Complex method call
	 */
	string demoMethod(1: string arg) throws (1: DemoException ire)

}