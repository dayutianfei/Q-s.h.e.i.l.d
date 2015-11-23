package cn.dayutianfei.lusearch.server.searchmanager;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 * @author wzy
 * 缓存searcherManager，是线程安全的，针对检索过程中的lucene文件读取句柄进行缓存和管理
 * 使用LRU策略进行缓存的替换
 */
public class LcSearchManager {
    
    private static Logger LOG = Logger.getLogger(LcSearchManager.class);
    
    private static LcSearchManager manager = null;
	private SearcherFactory fac=new SearcherFactory(); 
	private static final int maxNum=1024 * 2000 ; // 缓存的最大Shard操作类数量
	//key:shard完整路径，使用name作为标识 value:该lucene文件对应的检索实现类
	private ConcurrentHashMap<String ,SearcherManager> lcSearcherMap = null;
	private LinkedHashMap<String, String> LRUCache = null;
	private Thread assistant = null;
	
	/**
	 *  构造函数，实例化所有的变量
	 */
	@SuppressWarnings("serial")
    private LcSearchManager(){
	    lcSearcherMap=new ConcurrentHashMap<String ,SearcherManager>(maxNum);
		LRUCache = new LinkedHashMap<String, String>((int) Math.ceil(maxNum / 0.75f) + 1, 0.75f, true) {
            @Override
	        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                if(size() > maxNum){
                    LOG.info("remove the oldest searchManager : " + eldest.getKey() + " created on : " + eldest.getValue());
                    return true;
                }else{
                    return false;
                }
	        }
	    };
	    assistant = new Thread(new LcSearchManagerCacheMgtThread());
	    assistant.start();
	    LOG.info("the lcSearchManager's assistant start to work");
	}
	/**
	 * 获取检索使用的LcSearchManger，通过该manager可以获取具体检索
	 * 某个文件的操作类（或为IndexReader，或为SearchManager）
	 * @return
	 */
	public static LcSearchManager getInstance(){
	    if(null == manager){
	        manager = new LcSearchManager();
	    }
	    return manager;
	}
	
	/**
	 * 获取检索某个具体文件的执行类
	 * @param db 数据库名（不区分大小写）
	 * @param tbl 表名（不区分大小写）
	 * @param shardName lc文件的标识（为全路经），区分大小写
	 * @return 如果执行成功返回具体的句柄对象，如果失败，返回null
	 */
    public SearcherManager getHandler(String db, String tbl, String shardName) {
        String mark = db + ":" + tbl + ":" + shardName;
        synchronized (this) {
            if (lcSearcherMap.containsKey(mark)) {
                SearcherManager is = lcSearcherMap.get(mark);
                LRUCache.put(mark, Long.toString(System.currentTimeMillis()));
                return is;
            }
        }
        Path shardPath = Paths.get(shardName); // TODO: shardName = path
        Directory dir = null;
        SearcherManager sm = null;
        try {
            dir = FSDirectory.open(shardPath);
            sm = new SearcherManager(dir, this.fac);
            if(!lcSearcherMap.contains(mark)){
                lcSearcherMap.putIfAbsent(mark, sm);
            }
        }
        catch (IOException e) {
            LOG.error(e.getMessage());
        }
        LRUCache.put(mark, Long.toString(System.currentTimeMillis()));
        return sm;
    }
	
	/**
	 * 更新本地缓存的检索Shard的操作类
	 */
	public static void update(){
	    
	}
	
	public static void main(String[] args) {
	    LcSearchManager sc = LcSearchManager.getInstance();
	    for(int i = 0; i< (maxNum+6); i++){
	        sc.LRUCache.put(""+i, ""+System.currentTimeMillis());
	        if(i==(maxNum -2)){
	            sc.LRUCache.put(""+2, ""+System.currentTimeMillis());
	        }
	    }
	}
}
