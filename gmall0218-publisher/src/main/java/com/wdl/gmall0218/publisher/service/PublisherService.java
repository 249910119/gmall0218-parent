package com.wdl.gmall0218.publisher.service;

import org.springframework.stereotype.Repository;

import java.util.Map;

public interface PublisherService {

    /**
     * ES查询数据
     * @param date
     * @param keyword
     * @param pageSize
     * @param pageNo
     * @return
     */
    public Map getSaleDetail(String date, String keyword, int pageSize, int pageNo);
    /**
     * 查询日活总数
     * @param date
     * @return
     */
    public int getDauTotal(String date );

    /**
     * 查询日活分时明细
     * @param date
     * @return
     */
    public Map getDauHours(String date );

    /**
     * 查询总交易额
     * @param date
     * @return
     */
    public Double getOrderAmountTotal(String date);

    /**
     * 查询分时交易额
     * @param date
     * @return
     */
    public Map getOrderAmountHour(String date);
}
