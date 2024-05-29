package com.weimob.mp.merchant.backstage.server.service.impl;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.druid.sql.visitor.functions.If;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.google.common.collect.Maps;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.currency.DefaultCurrencyReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.share.GetBosDataByKeyReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.share.GetIdsByKeyReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.shop.ShopInfoReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.currency.CurrencyInfo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.share.GetBosDataByKeyResVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.share.GetIdsByKeyResVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.shop.ShopDetailVo;
import com.weimob.mp.merchant.backstage.common.exception.ServerException;
import com.weimob.mp.merchant.backstage.server.service.BosDataService;
import com.weimob.mp.merchant.backstage.server.service.CurrencyService;
import com.weimob.mp.merchant.backstage.server.service.ShareService;
import com.weimob.mp.merchant.backstage.server.service.ShopHomeService;
import com.weimob.mp.merchant.backstage.server.service.cache.ShopDataCacheManager;
import com.weimob.mp.merchant.backstage.upload.util.BeanCopyUtils;
import com.weimob.saas.common.spf.core.lib.aop.ErrorWrapper;
import com.weimob.saas.common.spf.core.lib.context.GlobalErr;
import com.weimob.saas.common.spf.core.lib.context.RCode;
import com.weimob.zipkin.ZipkinContext;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static com.weimob.mp.merchant.backstage.server.constant.MpBizCode.BOS_PRIVILEGE_ERROR;
import static com.weimob.mp.merchant.backstage.server.constant.MpBizCode.BOS_STATUS_ERROR;
import static com.weimob.mp.merchant.backstage.server.constant.MpBizCode.VID_PRIVILEGE_ERROR;
import static java.util.stream.Collectors.toList;

@Service
public class BosDataServiceImpl implements BosDataService {

    @Autowired
    private ShopHomeService shopHomeService;
    @Autowired
    private ShareService shareService;
    @Autowired
    private CurrencyService currencyService;
    @Autowired
    private ShopDataCacheManager shopDataCacheManager;

    @Override
    public GetBosDataByKeyResVo getBosDataByKey(GetBosDataByKeyReqVo reqVo) {

        GetBosDataByKeyResVo cacheBosData = shopDataCacheManager.getL1CacheBosData(reqVo.getWid(), reqVo.getKey());
        if (Objects.nonNull(cacheBosData)) {
            return cacheBosData;
        }
        GetBosDataByKeyResVo resVo = new GetBosDataByKeyResVo();
        try {
            GetIdsByKeyReqVo idsByKeyReqVo = new GetIdsByKeyReqVo();
            idsByKeyReqVo.setKey(reqVo.getKey());
            idsByKeyReqVo.setWid(reqVo.getWid());
            GetIdsByKeyResVo getIdsByKeyResVo = shareService.getIdsByKey(idsByKeyReqVo).getData();
            resVo.setGetIdsByKey(getIdsByKeyResVo);
            if (getIdsByKeyResVo != null && getIdsByKeyResVo.getBosId() != null) {
                if (!getIdsByKeyResVo.getBosId().equals(reqVo.getBosId())) {
                    throw new IllegalArgumentException("bosId和key不一致");
                }

                ShopInfoReqVo shopInfoReqVo = new ShopInfoReqVo();
                shopInfoReqVo.setBosId(getIdsByKeyResVo.getBosId());
                resVo.setShopInfo(shopHomeService.shopInfo(shopInfoReqVo).getData());

                DefaultCurrencyReqVo defaultCurrencyReqVo = new DefaultCurrencyReqVo();
                defaultCurrencyReqVo.setBosId(getIdsByKeyResVo.getBosId());
                resVo.setDefaultCurrency(currencyService.defaultCurrency(defaultCurrencyReqVo).getData());
            }
        } catch (Exception e) {
            List<RCode> errorCodes = GlobalErr.getErrorCodes();
            if (Objects.nonNull(errorCodes)) {
                List<Long> errCodes = errorCodes.stream().map(RCode::getErrorcode).collect(toList());
                List<Long> codeList = Arrays.asList(BOS_PRIVILEGE_ERROR.getErrcode(), BOS_STATUS_ERROR.getErrcode(),
                        VID_PRIVILEGE_ERROR.getErrcode());
                //有交集则抛出捕获到的业务异常
                if (!Collections.disjoint(codeList,errCodes)) {
                    throw e;
                }
            }
            resVo = fallbackHandler(reqVo);
            if (Objects.isNull(resVo.getDefaultCurrency()) && Objects.isNull(resVo.getGetIdsByKey()) && Objects
                    .isNull(resVo.getShopInfo())) {
                throw e;
            }
        }
        shopDataCacheManager.setL1AndL2CacheBosData(reqVo.getWid(),reqVo.getKey(),resVo);
        return resVo;
    }

    private GetBosDataByKeyResVo fallbackHandler(GetBosDataByKeyReqVo reqVo){
        String globalTicket = ZipkinContext.getContext().getGlobalTicket();
        Map<String, Object> map = Maps.newHashMap();
        map.put("errTicketId",globalTicket);
        Cat.logTransaction("fallback","fallbackHandler.bosdata",
                System.currentTimeMillis(),Message.FAIL, map);
        GetBosDataByKeyResVo resVo = shopDataCacheManager.getL2CacheBosData(reqVo.getWid(), reqVo.getKey());
        if(Objects.isNull(resVo)){
            resVo = new GetBosDataByKeyResVo();
            return resVo;
        }
        return resVo;
    }
}
