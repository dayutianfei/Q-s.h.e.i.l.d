package cn.dayutianfei.lucene.analyzer;

public class AnalyzerExistsDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		String analyzerClass = "cn.dayutianfei.xxx.Analyzer";
		String analyzerClass = "org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer";
		try{
			Class.forName(analyzerClass);
			System.out.println("the class does exists");
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		
	}

}
