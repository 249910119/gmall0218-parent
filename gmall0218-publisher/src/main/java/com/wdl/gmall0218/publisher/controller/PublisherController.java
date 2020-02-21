package com.wdl.gmall0218.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.wdl.gmall0218.publisher.bean.Option;
import com.wdl.gmall0218.publisher.bean.Stat;
import com.wdl.gmall0218.publisher.service.PublisherService;
import org.apache.commons.cli.OptionGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyword){
        Map saleDetailMap = publisherService.getSaleDetail(date, keyword, size, startpage);

        Long total = (Long)saleDetailMap.get("total");
        List<Map> saleDetailList = (List)saleDetailMap.get("detail");
        Map ageMap =(Map) saleDetailMap.get("ageMap");
        Map genderMap =(Map) saleDetailMap.get("genderMap");

        Long femaleCount = (Long) genderMap.get("F");
        Long maleCount = (Long) genderMap.get("M");
        double femaleRate = Math.round(femaleCount * 1000D / total) / 10D;
        double maleRate = Math.round(maleCount * 1000D / total) / 10D;
        List<Option> genderOptions=new ArrayList<>();
        genderOptions.add( new Option("男", maleRate));
        genderOptions.add( new Option("女", femaleRate));
        Stat genderStat = new Stat("性别占比", genderOptions);

        Long age_20Count = 0L;
        Long age20_30Count = 0L;
        Long age30_Count = 0L;

        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String key = (String) entry.getKey();
            int age = Integer.parseInt(key);
            Long ageCount = (Long) entry.getValue();
            if(age <20){
                age_20Count += ageCount;
            }else   if(age >= 20 && age < 30){
                age20_30Count += ageCount;
            }else{
                age30_Count += ageCount;
            }
        }
        Double age_20rate=0D;
        Double age20_30rate=0D;
        Double age30_rate=0D;

        age_20rate = Math.round(age_20Count * 1000D / total) / 10D;
        age20_30rate = Math.round(age20_30Count * 1000D / total) / 10D;
        age30_rate = Math.round(age30_Count * 1000D / total) / 10D;

        List<Option> ageOptions=new ArrayList<>();
        ageOptions.add( new Option("20岁以下",age_20rate));
        ageOptions.add( new Option("20岁到30岁",age20_30rate));
        ageOptions.add( new Option("30岁以上",age30_rate));

        Stat ageStat= new Stat("年龄占比", ageOptions);

        List<Stat> statList = new ArrayList<>();
        statList.add(genderStat);
        statList.add(ageStat);

        Map resultMap=new HashMap();
        resultMap.put("total",saleDetailMap.get("total"));
        resultMap.put("stat",statList);
        resultMap.put("detail",saleDetailMap.get("list"));


        return JSON.toJSONString(resultMap);
    }


    @GetMapping("realtime-hour")
    public String getHourTotal(@RequestParam("id")String id, @RequestParam("date") String date){

        if ("dau".equals(id)){
            Map<String, Map> map = new HashMap<>();

            Map dauHours = publisherService.getDauHours(date);

            map.put("today", dauHours);

            String jsonString = JSON.toJSONString(map);

            return jsonString;
        } else if(id.equals("order_amount")){
            //交易额
            Map orderHourTDMap = publisherService.getOrderAmountHour(date);
            Map<String, Map> hourMap = new HashMap();
            hourMap.put("today", orderHourTDMap);

            return JSON.toJSONString(hourMap);
        }
        return  null;
    }

    /**
     * 统计总数
     * @param date
     * @return
     */
    @GetMapping(path = "realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){

        List<Map> totalList=new ArrayList<>();

        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        int dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);

        Map ordMap = new HashMap();
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);
        ordMap.put("id", "ordAccount");
        ordMap.put("name", "交易额");
        ordMap.put("value", orderAmountTotal);
        totalList.add(ordMap);

        String jsonString = JSON.toJSONString(totalList);

        return jsonString;
    }
}
