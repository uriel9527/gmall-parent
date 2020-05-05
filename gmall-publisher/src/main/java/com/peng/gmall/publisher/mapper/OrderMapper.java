package com.peng.gmall.publisher.mapper;

import com.peng.gmall.publisher.bean.OrderHourAmount;

import java.util.List;

public interface OrderMapper {

    public Double getOrderAmount(String date);

    public List<OrderHourAmount> getOrderHourAmount(String date);

}
