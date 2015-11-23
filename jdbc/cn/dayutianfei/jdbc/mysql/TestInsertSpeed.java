package cn.dayutianfei.jdbc.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class TestInsertSpeed  implements Runnable{

	 private String connectStr;
	 private String username;
	 private String password;
	 private String insert_sql;
	 
	 private String[] hosts = new String[]{"host01","host02","host03","host04","host05",
	    		"host06","host07","host08","host09","host10",
	    		"host11","host12","host13","host14","host15",
	    		"host16","host17","host18","host19","host20",
	    		"host21","host22","host23","host24"};
	    
	    private String[] sources = new String[]{"clint","servr"};
	    
	    private String[] dbs = new String[]{"db01","db02","db03"};
	    
	    private String[] tbls = new String[]{"tbl01","tbl02","tbl03","tbl04","tbl05","tbl06"};
	
	public TestInsertSpeed(){
		connectStr = "jdbc:mysql://172.16.8.34:3306/singlebt";
		connectStr += "?useServerPrepStmts=false&rewriteBatchedStatements=true";
		insert_sql = "INSERT INTO test_big (id, host, source, db_name, table_name, load_number, load_size," +
        		"start_time, end_time, create_time, update_time ) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        username = "hive";
        password = "hive";
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int threadNumber = 24;
		TestInsertSpeed instance = new TestInsertSpeed();
		Thread[] threads = new Thread[threadNumber];
		for(int i = 0 ; i <threadNumber; i++){
			threads[i]=new Thread(instance,"thread-"+i);
		}
		for(int i = 0 ; i <threadNumber; i++){
			threads[i].start();
		}
	}

	@Override
	public void run() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(connectStr, username,password);
			conn.setAutoCommit(false); // 设置手动提交
			int count = 0;
	        long id = 1;
	        Random r = new Random();
	        PreparedStatement psts = conn.prepareStatement(insert_sql);
	        String starttime = "2015-01-11 00:00:00";
	        String endtime = getNextTime(starttime);
	        long start = System.currentTimeMillis();
	        while (true) {
	        	int nextHost = r.nextInt(hosts.length);
	        	int nextSource = r.nextInt(sources.length);
	        	int nextDb = r.nextInt(dbs.length);
	        	int nextTbl = r.nextInt(tbls.length);
	        	int nextLoadNumber = r.nextInt(10000000);
	        	int nextLoadSize = r.nextInt(512 * 1024 * 1024 );
	            psts.setLong(1, id);
	            psts.setString(2, hosts[nextHost]);
	            psts.setString(3, sources[nextSource]);
	            psts.setString(4, dbs[nextDb]);
	            psts.setString(5, tbls[nextTbl]);
	            psts.setLong(6, nextLoadNumber);
	            psts.setLong(7, nextLoadSize);
	            psts.setString(8, starttime);
	            psts.setString(9, endtime);
	            psts.setString(10, endtime);
	            psts.setString(11, endtime);
	            psts.addBatch();          // 加入批量处理
	            count++;
	            if(count%(hosts.length*sources.length*dbs.length*tbls.length)==0){
	            	 starttime=endtime;
	            	 endtime = getNextTime(starttime);
	            }
	            if(count>100){
	            	break;
	            }
	        }
	        System.out.println(Thread.currentThread().getName()+" making data cost : " + (System.currentTimeMillis() - start));
	        psts.executeBatch(); // 执行批量处理
	        conn.commit();  // 提交
	        System.out.println(Thread.currentThread().getName()+" All done : " + count + " cost "+(System.currentTimeMillis() - start));
	        conn.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
	
	private static String getNextTime(String starttime){
    	DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String reStr = "0000-00-00 00:00:00";
    	try{ 
	    	Date dt = df.parse(starttime);
	    	Calendar rightNow = Calendar.getInstance();
	        rightNow.setTime(dt);
	        rightNow.add(Calendar.MINUTE,+1);//日期减1年
	        Date dt1=rightNow.getTime();
	        reStr = df.format(dt1);
    	}catch (Exception e){
    	}
        return reStr;
    }

}
