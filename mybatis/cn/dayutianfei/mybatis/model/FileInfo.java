package cn.dayutianfei.mybatis.model;

import java.io.Serializable;
import java.util.Map;

/**
 * 存储所有的文件信息
 * @author wzy { @see HdfsPartition.java}
 * @date 2015-11-25
 */
public class FileInfo implements Serializable {
    
    public enum IDrillerFileType{
        INDICE,  // 索引，包括Bloomfilter, lucene
        SOURCE, // 原始文件
        ATTACHEMENT, //Binary外存文件
        ASSIST_INDICE //内置辅助型文件
    }

    private static final long serialVersionUID = 1L;
    private long id; // 文件唯一标识，全局唯一
	private String fileName; //文件名
	private String filePath; //文件访问路径，可以为一个URL地址
	private IDrillerFileType fileType;
	private long updateTime; //文件更新时间
	private Map<String,String> params; //文件参数
	
	@Override
    public String toString() {
        return "FileInfo [id=" + id + ", fileName=" + fileName + ", filePath=" + filePath + ", fileType="
                + fileType + ", updateTime=" + updateTime + ", params=" + params + "]";
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public IDrillerFileType getFileType() {
        return fileType;
    }

    public void setFileType(IDrillerFileType fileType) {
        this.fileType = fileType;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
	
}