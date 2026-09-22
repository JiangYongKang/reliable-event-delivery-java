package com.github.highcumontoa.reliableeventdeliveryjava.delivery;

import com.github.highcumontoa.reliableeventdeliveryjava.domain.DeliveryEvent;

/** 投递客户端抽象，便于测试替换 */
public interface DeliveryClient {
    DeliveryResult deliver(DeliveryEvent event);
}
