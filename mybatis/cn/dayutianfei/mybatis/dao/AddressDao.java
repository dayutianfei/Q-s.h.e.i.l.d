package cn.dayutianfei.mybatis.dao;

import org.apache.ibatis.session.SqlSession;

import cn.dayutianfei.mybatis.map.AddressMapper;
import cn.dayutianfei.mybatis.model.Address;
import cn.dayutianfei.mybatis.util.MyBatisUtil;

public class AddressDao {

    public Address getAddress(int id) {
        SqlSession sqlSession = MyBatisUtil.getSqlSessionFactory().openSession();
        try {
            AddressMapper test = sqlSession.getMapper(AddressMapper.class);
            return test.selectAddressById(id);
        }
        finally {
            sqlSession.close();
        }
    }

}
