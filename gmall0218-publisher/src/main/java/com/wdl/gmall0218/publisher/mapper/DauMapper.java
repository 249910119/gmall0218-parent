package com.wdl.gmall0218.publisher.mapper;

import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface DauMapper {

    /**
     * 日活统计
     * @param date
     * @return
     */
    public Integer selectDauTotal(String date);

    /**
     * 日活分时统计
     * @param date
     * @return
     */
    public List<Map> selectDauTotalHourMap(String date);

}
