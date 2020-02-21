package com.wdl.gmall0218.publisher.mapper;

import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface OrderMapper {

    public Double getOrderAmountTotal(String date);

    public List<Map> getOrderAmountHour(String date);

}
