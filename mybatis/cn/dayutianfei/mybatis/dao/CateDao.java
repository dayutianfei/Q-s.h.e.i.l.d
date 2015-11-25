package com.fwhale.dao;

import java.util.List;
import java.util.Map;

import com.fwhale.model.Cate;



public interface CateDao {
    
	/**
     * 
     * @param label
     * @return
     */
	public int save(Cate cate);
	/**
	 * 
	 * @param id
	 * @return
	 */
	public int delete(int id);
	/**
	 * 
	 * @param id
	 * @return
	 */
	public int delCateFileByCateId(int id);
	/**
	 * 
	 * @param label
	 * @return
	 */
	public int update(Cate cate);
	/**
	 * 
	 * @return
	 */
	public List<Cate> getByUserId(int userId);
	/**
	 * 
	 * @param name
	 * @return
	 */
	public Cate getByNameAndUserId(Map<String, Object> map);
	/**
     * 
     * @param id
     * @return
     */
    public Cate getById(int id);
    
    public Cate getByFileId(int id);
    
    public int getNumByCateId(int cateId);
    
}