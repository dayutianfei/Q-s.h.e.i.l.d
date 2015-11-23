package cn.dayutianfei.common.license;

import cn.dayutianfei.common.license.generator.SystemIdFactory;
import cn.dayutianfei.common.license.rsa.RSAEncryDecry;

import java.io.File;
import java.io.IOException;

public class LicenseTest {
	public static void main(String[] args) throws InterruptedException, IOException {
		LicenseValidate licenseValidate = new LicenseValidate(new File("license.properties"));
		String codedLicense = licenseValidate.makeLicense(new SystemIdFactory().generateSystemId(), String.valueOf(System.currentTimeMillis()), String.valueOf(15L * 1000), "0");
		System.out.println(codedLicense);
//		System.exit(0);
		licenseValidate.write(codedLicense);
		int i = 0;
		licenseValidate.start();
		while(licenseValidate.isValid()){
			i++;
			System.out.println(i + " : " + RSAEncryDecry.decry(licenseValidate.read()));
			Thread.sleep(1000);
		}
	}
}
