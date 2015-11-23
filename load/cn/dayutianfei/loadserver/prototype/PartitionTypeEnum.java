package cn.dayutianfei.loadserver.prototype;


public enum PartitionTypeEnum {
	  NONE(0),
	  HASH(1),
	  INTERVAL(2),
	  PRECISE(3);
	  
	  private final int value;

	  private PartitionTypeEnum(int value) {
	    this.value = value;
	  }
	  
	  /**
	   * Get the integer value of this enum value.
	   */
	  public int getValue() {
	    return value;
	  }

	  /**
	   * Find a the enum type by its integer value.
	   * @return null if the value is not found.
	   */
	  public static PartitionTypeEnum findByValue(int value) { 
	    switch (value) {
	      case 0:
	        return NONE;
	      case 1:
	        return HASH;
	      case 2:
	        return INTERVAL;
	      case 3:
	        return PRECISE;
	      default:
	        return null;
	    }
	  }
	  
	  public static PartitionTypeEnum str2PartitionTypeEnum(String type){
		  if (type.equalsIgnoreCase("HASH")) {
			  return PartitionTypeEnum.HASH;
		  }else if(type.equalsIgnoreCase("INTERVAL")){
			  return PartitionTypeEnum.INTERVAL;
		  }else if (type.equalsIgnoreCase("PRECISE")) {
			  return PartitionTypeEnum.PRECISE;
		  }
		  return PartitionTypeEnum.NONE;
		  
	  }
}
