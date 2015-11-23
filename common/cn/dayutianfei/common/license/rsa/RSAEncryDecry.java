package cn.dayutianfei.common.license.rsa;

import java.math.BigInteger;

public class RSAEncryDecry {
	private static final BigInteger pubExponent = new BigInteger("65537");
	private static final BigInteger priExponent = new BigInteger("11170496940573898989748917550340319107849026860183397838892670401044994485686480413520070613451446621968231831385427177910531752296823119063175278232032747144951166408572170162257075644533564118774408305058140439219031401573711661929621456128535334056009967420699944571134169067612010772355021615915132326473");
	private static final BigInteger modulus = new BigInteger("150944506802967343936324702989000720282701375945533885395362668056347588372873168424920591297684011806996290625465307414169591639232349846194498599895408302891062443745151710797078104251974889229509645020908360781139604034099542227760726698028103047036570647988738188660922048431571057435333013712396352606473");

	public static String encry(String uncodedStr) {
		byte[] uncodedChars;
		uncodedChars = uncodedStr.getBytes();
		BigInteger uncodedBigInteger = new BigInteger(uncodedChars);
		BigInteger codedBigInteger = uncodedBigInteger.modPow(pubExponent, modulus);
		return codedBigInteger.toString();
	}
	
	public static String decry(String codedStr) {
		BigInteger codedBigInteger = new BigInteger(codedStr);
		BigInteger uncodedBigInteger = codedBigInteger.modPow(priExponent, modulus);
		byte [] uncodedChars = uncodedBigInteger.toByteArray();
		return new String(uncodedChars);
	}
}
