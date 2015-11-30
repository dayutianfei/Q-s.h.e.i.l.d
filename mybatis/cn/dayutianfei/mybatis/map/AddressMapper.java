package cn.dayutianfei.mybatis.map;

import org.apache.ibatis.annotations.Select;

import cn.dayutianfei.mybatis.model.Address;


public interface AddressMapper {
    @Select("select addr_id as addrId, street, city, state, zip, country from ADDRESSES where addr_id=#{id}")
    Address selectAddressById(int id);
}
