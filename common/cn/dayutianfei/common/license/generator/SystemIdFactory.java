package cn.dayutianfei.common.license.generator;

import java.net.InetAddress;

//import com.jniwrapper.util.ProcessorInfo;

/**
 * @author macbookpro
 */
public class SystemIdFactory {

	public static final int HD_FIELD_LENGTH = "0000".length();

	/**
	 * Host ID<br>
	 * 
	 * @return
	 */
	public String generateHostId() {
		String hostName = null;
		try {
			InetAddress localInetAddress = InetAddress.getLocalHost();
			hostName = localInetAddress.getHostName();
		} catch (Exception e) {
			e.printStackTrace();
		}
		int hashCode = ((hostName == null) || "".equals((hostName))) ? 0 : Math
				.abs(hostName.hashCode());
		String hostId = String.format("%02X", new Object[] { new Integer(
				hashCode % 256) });
		return hostId;
	}

	public String generateMacAddress() {
		return new MACAddressUtil().getMacAddress();
	}

	public String generateHDId() {
		String home = System.getenv("HOME");
		String user = System.getenv("USER");
		String logname = System.getenv("LOGNAME");
		String shell = System.getenv("SHELL");
		String currentUse = null;
		if (home != null) {
			currentUse = home;
		} else if (user != null) {
			currentUse = user;
		} else if (logname != null) {
			currentUse = logname;
		} else if (shell != null) {
			currentUse = shell;
		} else {
			currentUse = "0000";
		}
		String hdId = Integer.toString(Math.abs(currentUse.hashCode()));
		int length = currentUse.length();
		if (length > HD_FIELD_LENGTH)
			hdId = hdId.substring(0, HD_FIELD_LENGTH);
		else if (length < HD_FIELD_LENGTH)
			hdId = hdId + "FEED".substring(0, HD_FIELD_LENGTH - length);
		return hdId;
	}

	public String generateSystemId() {

		return generateHostId() //
				+ generateMacAddress() //
				+ generateHDId();
	}

	public static void main(String[] args) throws Exception {
		System.out.println(new SystemIdFactory().generateHostId());
		System.out.println(new SystemIdFactory().generateMacAddress());
		System.out.println(new SystemIdFactory().generateHDId());
	}
}
