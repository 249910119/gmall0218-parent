package com.wdl.gmall0218.publisher.service.impl;

import com.wdl.gmall0218.common.GmallConstant;
import com.wdl.gmall0218.publisher.mapper.DauMapper;
import com.wdl.gmall0218.publisher.mapper.OrderMapper;
import com.wdl.gmall0218.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    public Map getSaleDetail(String date, String keyword, int pageSize, int pageNo){

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // TODO 过滤、匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);


        // TODO 性别聚合
        TermsAggregationBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(100);
        searchSourceBuilder.aggregation(genderAggs);
        // TODO 年龄聚合
        TermsAggregationBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(2);
        searchSourceBuilder.aggregation(ageAggs);
        // TODO 行号 = （页面-1） * 每页行数
        searchSourceBuilder.from((pageNo) * pageSize);
        searchSourceBuilder.size(pageSize);

        Search build = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE_DETAIL).addType("_doc").build();

        // TODO 需要总数，明细，2个聚合的结果
        Map resultMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(build);

            Long total = searchResult.getTotal();

            List<Map> saleDetailList=new ArrayList<>();
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleDetailList.add(hit.source);
            }

            Map genderMap=new HashMap();
            List<TermsAggregation.Entry> userGender = searchResult.getAggregations().getTermsAggregation("groupby_user_gender").getBuckets();
            for (TermsAggregation.Entry entry : userGender) {
                genderMap.put(entry.getKey(), entry.getCount());
            }

            Map ageMap=new HashMap();
            List<TermsAggregation.Entry> userAge = searchResult.getAggregations().getTermsAggregation("groupby_user_age").getBuckets();
            for (TermsAggregation.Entry entry : userAge) {
                ageMap.put(entry.getKey(), entry.getCount());
            }

            resultMap.put("total",total);
            resultMap.put("list",saleDetailList);
            resultMap.put("ageMap",ageMap);
            resultMap.put("genderMap",genderMap);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    @Override
    public int getDauTotal(String date) {
        Integer total = dauMapper.selectDauTotal(date);
        return total;
    }

    @Override
    public Map getDauHours(String date) {

        List<Map> dauTotalHourMap = dauMapper.selectDauTotalHourMap(date);

        Map map = new HashMap<>();

        for (Map dauMap : dauTotalHourMap) {
            String loghour = (String) dauMap.get("LOGHOUR");
            Long ct = (Long) dauMap.get("CT");

            map.put(loghour, ct);
        }

        return map;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.getOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {

        List<Map> orderAmountHour = orderMapper.getOrderAmountHour(date);

        Map map = new HashMap();

        for (Map ordMap : orderAmountHour) {
            String createHour = (String) ordMap.get("CREATE_HOUR");
            BigDecimal orderAmount = (BigDecimal) ordMap.get("ORDER_AMOUNT");
            map.put(createHour, orderAmount);
        }
        return map;
    }

}
