package cn.dayutianfei.common.license;

import cn.dayutianfei.common.license.rsa.RSAEncryDecry;
import cn.dayutianfei.common.license.validate.Validate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class LicenseValidate extends Thread{
	private File licenseFile;
	private static final String sep = " ";
	private static final long UPDATE_TIME = 1000 * 60 * 60;
	protected final static Logger LOG = Logger.getLogger(LicenseValidate.class);
	
	public LicenseValidate(File licenseFile) {
		super();
		this.licenseFile = licenseFile;
	}
	
	public File getLicenseFile() {
		return licenseFile;
	}
	
	public void setLicenseFile(File licenseFile) {
		this.licenseFile = licenseFile;
	}
	
	public boolean isValid() {
		String license = read();
		if (license != null && !"".equals(license)){
			String uncodedLicense = RSAEncryDecry.decry(license);
			if (uncodedLicense != null && !"".equals(uncodedLicense)){
				String[] strs = uncodedLicense.split(sep);
				if(strs.length == 4){
					return Validate.systemIdValidate(strs[0]) && Validate.isOutOfDate(strs[1], strs[2], strs[3]);
				}
			}
		}
		return false;
	}
	
	public void run(){
		while(true){
			long t = System.currentTimeMillis();
			try {
				Thread.sleep(UPDATE_TIME);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
			}
			long timeInc = System.currentTimeMillis() - t;
			String license = read();
			if (license != null && !"".equals(license)){
				String uncodedLicense = RSAEncryDecry.decry(license);
				if (uncodedLicense != null && !"".equals(uncodedLicense)){
					String[] strs = uncodedLicense.split(sep);
					if(strs.length == 4){
						long newRunnedTime = Long.parseLong(strs[3]) + timeInc;
						try {
							write(makeLicense(strs[0], strs[1], strs[2], String.valueOf(newRunnedTime)));
							if (! isValid()){
								LOG.error("your license exceed the time limit, please pay for it again.");
//								master.shutdown();
								break;
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	public String read() {
		try {
			InputStreamReader ireader = new InputStreamReader(new FileInputStream(licenseFile));
			BufferedReader bReader = new BufferedReader(ireader);
			String license = bReader.readLine();
			bReader.close();
			ireader.close();
			return license;
		} catch (Exception e) {}
		return null;
	}
	
	public void write(String str) throws IOException {
		FileWriter writer = new FileWriter(licenseFile);
		writer.write("");
		writer.append(str);
		writer.close();
	}
	
	public String makeLicense(String sysId, String startTime, String limitTime, String runnedTime) {
		return RSAEncryDecry.encry(sysId + sep + startTime + sep + limitTime + sep + runnedTime);
	}
}
