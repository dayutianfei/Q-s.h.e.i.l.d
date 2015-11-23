package cn.dayutianfei.common.license.generator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author macbookpro
 */
public class MACAddressUtil {

	String macAddress = null;
	String computeMacAddress = null;

	private static final Pattern HARDWARE_PATTERN = Pattern
			.compile(
					"(.*wireless.*)|(.*tunnel.*)|(.*atapi.*)|(.*bluetooth.*)|(.*vnic.*)|(.*vmnet.*)",
					2);

	public String getMacAddressWithNetworkInterface() {
		String computeMacAddress = null;
		try {
			Enumeration<NetworkInterface> networkInterfaces = NetworkInterface
					.getNetworkInterfaces();
			while (networkInterfaces.hasMoreElements()) {
				NetworkInterface ni = networkInterfaces.nextElement();
				if (ni != null && !ni.isVirtual() && !ni.isLoopback()
						&& ni.isUp()) {
					byte[] hardwareAddress = ni.getHardwareAddress();
					if (hardwareAddress != null && hardwareAddress.length != 2) {
						boolean isMacAddressLegal = false;
						for (byte b : hardwareAddress) {
							if (b <= 0) {
								isMacAddressLegal = true;
								break;
							}
						}

						if (isMacAddressLegal) {
							String hardwareName = ni.getDisplayName();
							if (hardwareName != null
									&& hardwareName.length() != 0) {
								Matcher matcher = HARDWARE_PATTERN
										.matcher(hardwareName);
								if (!matcher.lookingAt()) {
									computeMacAddress = String
											.format("%02x%02x",
													new Object[] {//
															Byte.valueOf(hardwareAddress[(hardwareAddress.length - 2)]), //
															Byte.valueOf(hardwareAddress[(hardwareAddress.length - 1)]) //
													});
									this.macAddress = buildMacAddress(hardwareAddress);
								}
							}
						}
					}
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
		}
		return (computeMacAddress == null ? "0000" : computeMacAddress);
	}

	public String buildMacAddress(byte[] bytes) {
		StringBuffer sb = new StringBuffer();
		if (bytes != null) {
			for (byte b : bytes) {
				sb.append(String.format("%02x", b) + ":");
			}
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}

	public String executeCommand(String[] commands) {
		Process process = null;
		BufferedReader br = null;
		String returnValue = null;
		try {
			process = Runtime.getRuntime().exec(commands);
			br = new BufferedReader(new InputStreamReader(
					process.getInputStream()), 128);
			returnValue = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (process != null) {
				try {
					if (br != null) {
						br.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					process.getErrorStream().close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					process.getOutputStream().close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return returnValue;
	}

	public String getMacAddressWithOS() {
		Process process = null;
		String macAddress = null;
		BufferedReader br = null;
		String osName = System.getProperty("os.name");
		if (osName != null) {
			try {
				if (osName.startsWith("Windows")) {// Windows
					process = Runtime.getRuntime().exec(
							new String[] { "ipconfig", "/all" }, null);
				} else if ((osName.startsWith("Solaris"))
						|| (osName.startsWith("SunOS"))) { // Sun
					String hostName = executeCommand(new String[] { "uname",
							"-n" });
					if (hostName != null) {
						process = Runtime.getRuntime().exec(
								new String[] { "/usr/sbin/arp", hostName },
								null);
					}
				} else if (new File("/usr/sbin/lanscan").exists()) {// HP-UX
					process = Runtime.getRuntime().exec(
							new String[] { "/usr/sbin/lanscan" }, null);
				} else if (new File("/sbin/ifconfig").exists()) {// Unix,Linux,Mac
					process = Runtime.getRuntime().exec(
							new String[] { "/sbin/ifconfig", "-a" }, null);
				}
				// end if

				if (process != null) {
					br = new BufferedReader(new InputStreamReader(
							process.getInputStream()), 128);
					String line = null;
					while ((line = br.readLine()) != null) {
						macAddress = macAddressParser(line);
						if (macAddress != null
								&& Hex.parseShort(macAddress) != 0xff) {
							this.macAddress = macAddress;
							break;
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (process != null) {
					try {
						if (br != null) {
							br.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						process.getErrorStream().close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						process.getOutputStream().close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		String computeMacAddress = null;
		int length = macAddress != null ? macAddress.length() : 0;
		if (length >= 5) {
			computeMacAddress = macAddress.substring(length - 5, length - 3)
					+ macAddress.substring(length - 2, length);
		}
		return (computeMacAddress != null ? computeMacAddress : "0000");
	}

	/**
	 * The MAC address parser attempts to find the following patterns:
	 * 
	 * <ul>
	 * <li>.{1,2}:.{1,2}:.{1,2}:.{1,2}:.{1,2}:.{1,2}</li>
	 * <li>.{1,2}-.{1,2}-.{1,2}-.{1,2}-.{1,2}-.{1,2}</li>
	 * </ul>
	 * 
	 * Attempts to find a pattern in the given String.
	 * 
	 * @see <a href="http://johannburkard.de/software/uuid/">UUID</a>
	 * @author <a href="mailto:jb@eaio.com">Johann Burkard</a>
	 * @version $Id: MACAddressParser.java 1888 2009-03-15 12:43:24Z johann $
	 * 
	 * @param in
	 *            the String, may not be <code>null</code>
	 * 
	 * @return the substring that matches this pattern or <code>null</code>
	 * 
	 */
	public String macAddressParser(String line) {
		String out = line;

		int hexStart = out.indexOf("0x");
		if (hexStart != -1 && out.indexOf("ETHER") != -1) {
			int hexEnd = out.indexOf(' ', hexStart);
			if (hexEnd > hexStart + 2) {
				out = out.substring(hexStart, hexEnd);
			}
		} else {
			int octets = 0;
			int lastIndex, old, end;
			if (out.indexOf('-') > -1) {
				out = out.replace('-', ':');
			}
			lastIndex = out.lastIndexOf(':');
			if (lastIndex > out.length() - 2) {
				out = null;
			} else {
				end = Math.min(out.length(), lastIndex + 3);
				++octets;
				old = lastIndex;
				while (octets != 5 && lastIndex != -1 && lastIndex > 1) {
					lastIndex = out.lastIndexOf(':', --lastIndex);
					if (old - lastIndex == 3 || old - lastIndex == 2) {
						++octets;
						old = lastIndex;
					}
				}
				if (octets == 5 && lastIndex > 1) {
					out = out.substring(lastIndex - 2, end).trim();
				} else {
					out = null;
				}
			}
		}

		if (out != null && out.startsWith("0x")) {
			out = out.substring(2);
		}

		return out;
	}

	public String getMacAddress() {
		if (this.computeMacAddress != null) {
			return computeMacAddress;
		}
		String macAddr = getMacAddressWithNetworkInterface();
		if (macAddr == null || "0000".equals(macAddr)) {
			macAddr = getMacAddressWithOS();
		}
		computeMacAddress = macAddr;
		return macAddr;
	}

}
