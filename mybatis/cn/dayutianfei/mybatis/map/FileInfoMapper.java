package cn.dayutianfei.mybatis.map;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import cn.dayutianfei.mybatis.model.FileInfo;

public interface FileInfoMapper {
    @Select("select * from students")
    @Results({ @Result(id = true, column = "stud_id", property = "studId"),
              @Result(column = "name", property = "name"), @Result(column = "email", property = "email"),
              @Result(column = "addr_id", property = "address.addrId") })
    List<FileInfo> findAllFileInfos();


    @Select("select stud_id as studId, name, email, addr_id as 'address.addrId', phone from students")
    List<Map<String, Object>> findAllStudentsMap();


    @Select("select stud_id as studId, name, email, addr_id as 'address.addrId', phone from students where stud_id=#{id}")
    FileInfo findStudentById(Integer id);


    @Select("select stud_id as studId, name, email, addr_id as 'address.addrId', phone from students where stud_id=#{id}")
    Map<String, Object> findStudentMapById(Integer id);


    @Select("select stud_id, name, email, a.addr_id, street, city, state, zip, country"
            + " FROM students s left outer join addresses a on s.addr_id=a.addr_id"
            + " where stud_id=#{studId} ")
    @ResultMap("com.mybatis3.mappers.StudentMapper.StudentWithAddressResult")
    FileInfo selectStudentWithAddress(int studId);


    @Insert("insert into students(name,email,addr_id, phone) values(#{name},#{email},#{address.addrId},#{phone})")
    @Options(useGeneratedKeys = true, keyProperty = "studId")
    void insertStudent(FileInfo student);


    @Insert("insert into students(name,email,addr_id, phone) values(#{name},#{email},#{address.addrId},#{phone})")
    @Options(useGeneratedKeys = true, keyProperty = "studId")
    void insertStudentWithMap(Map<String, Object> map);


    @Update("update students set name=#{name}, email=#{email}, phone=#{phone} where stud_id=#{studId}")
    void updateFileInfo(FileInfo student);


    @Delete("delete from file_info where file_id=#{fileid}")
    int deleteFileInfo(int fileid);
}
