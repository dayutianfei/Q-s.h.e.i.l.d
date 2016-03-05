package cn.dayutianfei.main;

import cn.dayutianfei.archive.MultiThreandsContainer;

/**
 * 加载入口，运行该类进行数据加载
 * @author dayutianfei
 *
 */
public class LoadMain {
	public static void main(String[] args){
		MultiThreandsContainer container = new MultiThreandsContainer();
		container.init(10);
		container.startToWork();
		while(true){
			if(MultiThreandsContainer.jobHasDone.get()==10){
				container.close();
				break;
			}
		}
	}
}
