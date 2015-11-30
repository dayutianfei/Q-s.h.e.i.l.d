package cn.dayutianfei.mybatis.map;

import org.apache.ibatis.annotations.Delete;

import cn.dayutianfei.mybatis.model.FileInfo;

public interface FileInfoMapper {
    public FileInfo findById(int id);

    @Delete("delete from file_info where file_id=#{fileid}")
    int deleteFileInfo(int fileid);
}
