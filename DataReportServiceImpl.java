package com.weimob.mp.merchant.backstage.server.service.impl;

import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.weimob.dc.guide.core.api.guider.GuideGuiderExportService;
import com.weimob.dc.guide.core.api.guider.request.GetSingleGuiderByWidAndVidReqDTO;
import com.weimob.dc.guide.core.api.guider.response.GuiderDTO;
import com.weimob.mp.merchant.api.product.dto.ProductInstanceFlowDTO;
import com.weimob.mp.merchant.backstage.api.domain.dto.ModelInfoDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.RootNode;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.shop.GetDataOverviewDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.shop.GetShopDataReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.shop.GetShopDataTargetReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.shop.queryTargetDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.mobileshare.mina.BusTargeDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.mobileshare.mina.DataOverviewVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.mobileshare.mina.dataoverview.TargetInfoDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.shop.ShopDataResVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.shop.ShopDataTargetResVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.shop.TargetDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.shop.TargetGroupDto;
import com.weimob.mp.merchant.backstage.dao.mappers.WorkbenchMapper;
import com.weimob.mp.merchant.backstage.dao.mappers.WorkbenchMenuMapper;
import com.weimob.mp.merchant.backstage.dao.model.TopbarOrder;
import com.weimob.mp.merchant.backstage.dao.model.Workbench;
import com.weimob.mp.merchant.backstage.dao.model.WorkbenchMenu;
import com.weimob.mp.merchant.backstage.server.constant.Constants;
import com.weimob.mp.merchant.backstage.server.constant.MpBizCode;
import com.weimob.mp.merchant.backstage.server.dto.HttpResp;
import com.weimob.mp.merchant.backstage.server.dto.ResponseVO;
import com.weimob.mp.merchant.backstage.server.enums.ShopDataChannelTypeEnum;
import com.weimob.mp.merchant.backstage.server.enums.SystemChannelEnum;
import com.weimob.mp.merchant.backstage.server.enums.WorkbenchEnum;
import com.weimob.mp.merchant.backstage.server.facade.PrivilegeFacade;
import com.weimob.mp.merchant.backstage.server.facade.ProductFacade;
import com.weimob.mp.merchant.backstage.server.facade.StructureFacade;
import com.weimob.mp.merchant.backstage.server.service.DataReportService;
import com.weimob.mp.merchant.backstage.server.service.TopbarOrderService;
import com.weimob.mp.merchant.backstage.server.util.ApiTemplate;
import com.weimob.mp.merchant.backstage.server.util.ExceptionUtils;
import com.weimob.mp.merchant.backstage.server.util.SoaTransformUtil;
import com.weimob.mp.merchant.common.commonpage.BasePageWrapper;
import com.weimob.mp.merchant.common.exception.MerchantErrorCode;
import com.weimob.mp.merchant.common.util.BeanCopyUtils;
import com.weimob.mp.merchant.op.platform.ability.api.FormInstanceAbility;
import com.weimob.mp.merchant.op.platform.ability.request.FormInstancePageQuery;
import com.weimob.mp.merchant.op.platform.common.dto.form.FormInstanceDetailDTO;
import com.weimob.mp.merchant.privilege.ability.AccountInfoExportService;
import com.weimob.mp.merchant.privilege.ability.IntegrationExportService;
import com.weimob.mp.merchant.privilege.ability.domain.dto.request.IntegrationQueryRequest;
import com.weimob.mp.merchant.privilege.ability.domain.dto.request.QueryAccountInfoRequest;
import com.weimob.mp.merchant.privilege.ability.domain.dto.response.AccountInfoDetailDTO;
import com.weimob.mp.merchant.privilege.ability.domain.dto.response.ProductInfoDTO;
import com.weimob.mp.merchant.privilege.ability.domain.dto.response.ProductInstanceIdShowDTO;
import com.weimob.mp.merchantstructure.ability.domain.response.QueryVidInfoResponseDto;
import com.weimob.saas.common.spf.core.lib.utils.GEPreconditions;
import com.weimob.saas.common.spf.core.lib.utils.SoaRespUtils;
import com.weimob.saas.mall.common.model.BasicInfo;
import com.weimob.saas.mall.common.response.HttpResponse;
import com.weimob.soa.common.response.SoaResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.weimob.mp.merchant.backstage.server.constant.Constants.*;
import static java.util.stream.Collectors.toList;

/**
 * 数据指标
 */
@Service
@EnableApolloConfig(value = "data-api.config")
@Slf4j
public class DataReportServiceImpl implements DataReportService {

    @Autowired
    private ApiTemplate apiUtil;
    @Autowired
    private ProductFacade productFacade;
    @Reference
    private IntegrationExportService integrationExportService;
    @Autowired
    private StructureFacade structureFacade;
    @Autowired
    private PrivilegeFacade privilegeFacade;
    @Autowired
    private WorkbenchMapper workbenchMapper;
    @Autowired
    private WorkbenchMenuMapper workbenchMenuMapper;
    @Autowired
    private TopbarOrderService topbarOrderService;
    @Reference
    private GuideGuiderExportService guideGuiderExportService;
    @Reference
    private AccountInfoExportService accountInfoExportService;
    @Reference
    private FormInstanceAbility formInstanceAbility;

    @Value("${dataApi.overView.appId}")
    private String appId;
    @Value("${dataApi.guiderGroupName.url}")
    private String groupNameUrl;
    @Value("${dataApi.guiderGroupName.key}")
    private String groupNameKey;
    @Value("${dataApi.guiderGroupName.secret}")
    private String groupNameSecret;
    @Value("${dataApi.guiderGroupName.dataKey}")
    private String datakey;

    private static final String targetConfigKey = "ruN";

    private static final String auxiliaryFunctionKey = "DATA_OVERVIEW";

    @Override
    public HttpResponse<List<ShopDataResVo>> getShopData(GetShopDataReqVo reqVo) {
        //BI入参vid 为0时替换为根节点
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
        if (Objects.isNull(rootNode)) {
            return SoaTransformUtil.success(new ArrayList<>());
        }
        Long rootVid = reqVo.getVid();
        Integer rootVidType = reqVo.getVidType();
        if (DEFAULT_VID.equals(reqVo.getVid())) {
            rootVid = rootNode.getVid();
            rootVidType = rootNode.getVidType();
        }
        List<ProductInstanceFlowDTO> productInstanceList;
        //融合节点时查询所有权限
        if (Constants.DEFAULT_VID.equals(reqVo.getVid()) ||
                (Objects.nonNull(rootNode) && rootNode.getVid().equals(reqVo.getVid()))) {
            if (Objects.isNull(reqVo.getVidPath())) {
                //set根节点vidPath值
                QueryVidInfoResponseDto vidInfo = structureFacade.getVidInfo(rootVid);
                reqVo.setVidPath(vidInfo.getPath());
            }
            productInstanceList = productFacade.getBosProductInstanceId(reqVo.getBosId());
        } else {
            productInstanceList = productFacade.getVidProductInstanceId(reqVo.getVid());
        }
        if (CollectionUtils.isEmpty(productInstanceList)) {
            ExceptionUtils.checkState(false,MpBizCode.NOT_FOUND_VID_BIPRODUCT);
        }
        List<ProductInstanceIdShowDTO> productInstanceIdShowDTOS = new ArrayList<>();
        //数据指标
        List<DataOverviewVo> dataOverviewVos = getTargetList(reqVo.getBosId(),ShopDataChannelTypeEnum.PC.getChannelType(),reqVo.getVidType());
        //根据导购角色过滤
        dataOverviewVos = filterByGuide(reqVo.getBosId(),rootVid,reqVo.getWid(),dataOverviewVos);
        //数据指标过滤产品实例
        List<Long> productIds = dataOverviewVos.stream().map(DataOverviewVo::getProductId).collect(toList());
        //(缩小查询范围)
        List<Long> productInstance = productInstanceList.stream()
                .filter(instance -> productIds.contains(instance.getProductId()))
                .map(ProductInstanceFlowDTO::getProductInstanceId)
                .collect(toList());
        if (Constants.DEFAULT_VID.equals(reqVo.getVid()) ||
                (Objects.nonNull(rootNode) && rootNode.getVid().equals(reqVo.getVid()))) {
            productInstanceIdShowDTOS.addAll(privilegeFacade.queryProductInstanceIdShow(reqVo.getWid(),reqVo.getBosId(),
                    DEFAULT_VID,DEFAULT_VID_TYPE,productInstance));
            productInstanceIdShowDTOS.addAll(privilegeFacade.queryProductInstanceIdShow(reqVo.getWid(),reqVo.getBosId(),
                    rootVid,rootVidType,productInstance));
        } else {
            productInstanceIdShowDTOS.addAll(privilegeFacade.queryProductInstanceIdShow(reqVo.getWid(),reqVo.getBosId(),
                    reqVo.getVid(),reqVo.getVidType(),productInstance));
        }
        List<Long> showInstance = productInstanceIdShowDTOS.stream()
                .filter(ProductInstanceIdShowDTO::getShow)
                .map(ProductInstanceIdShowDTO::getProductInstanceId).distinct().collect(toList());
        if (CollectionUtils.isEmpty(showInstance)) {
            ExceptionUtils.checkState(false,MpBizCode.NOT_FOUND_VID_BIPRODUCT);
        }
        Map<Long, Long> productMap = productInstanceList.stream()
                .filter(instance -> showInstance.contains(instance.getProductInstanceId()))
                .collect(Collectors.toMap(ProductInstanceFlowDTO::getProductId,ProductInstanceFlowDTO::getProductInstanceId));
        //节点可见数据指标
        dataOverviewVos = dataOverviewVos.stream()
                //权限二次过滤
                .filter(vo -> Objects.isNull(vo.getProductId()) || productMap.keySet().contains(vo.getProductId()))
                .collect(toList());
        //产品版本附属功能过滤
        dataOverviewVos = filterByAttachFunction(dataOverviewVos,productMap);
        //无可见指标隐藏店铺数据模块
        if (CollectionUtils.isEmpty(dataOverviewVos)) {
            ExceptionUtils.checkState(false,MpBizCode.NOT_FOUND_VID_BIPRODUCT);
        }
        //数据指标查询
        GetDataOverviewDto reqDto = new GetDataOverviewDto();
        reqDto.setBosId(reqVo.getBosId());
        reqDto.setVid(rootVid);
        reqDto.setVidType(rootVidType);
        reqDto.setVidPath(reqVo.getVidPath());
        List<queryTargetDto> queryTargetDtos = new ArrayList<>();
        final Long guiderVid = rootVid;
        dataOverviewVos.forEach(overView ->{
            List<String> keyList = overView.getBusTargets().stream().map(BusTargeDto::getKey).collect(toList());
            queryTargetDtos.add(new queryTargetDto(overView.getProductId(),
                    productMap.get(overView.getProductId()), overView.getUrl(),overView.getAppKey(),overView.getAppSecret(),keyList));
            if (SALES_PRODUCT_ID.equals(overView.getProductId()) && Boolean.TRUE.equals(overView.getPrivateGroup())) {
                String guiderId = getGuiderId(reqVo.getBosId(),reqVo.getWid(),guiderVid,productMap.get(overView.getProductId()));
                reqDto.setGuiderWid(reqVo.getWid());
                reqDto.setGuiderId(guiderId);
            }
        });
        reqDto.setQueryTargetDtos(queryTargetDtos);
        Map<String, Object> dataMap = getDataOverview(reqDto);
        List<ShopDataResVo> shopDataResVos = new ArrayList<>();
        //查询用户配置指标
        Workbench queryDto = BeanCopyUtils.copy(reqVo, Workbench.class);
        queryDto.setVid(rootVid);
        queryDto.setSystem(SystemChannelEnum.PC.getCode());
        queryDto.setModuleType(WorkbenchEnum.DATA_OVERVIEW.getCode());
        List<Workbench> workbenchList = workbenchMapper.selectList(queryDto);
        //如果有配置则取配置的key
        List<String> keyList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(workbenchList)) {
            List<Long> workbenchMenuIds = new ArrayList<>();
            for (Workbench workbench : workbenchList) {
                List<WorkbenchMenu> workbenchMenus = workbenchMenuMapper
                        .selectByWorkBenchId(workbench.getId(), reqVo.getWid());
                keyList.addAll(workbenchMenus.stream().map(WorkbenchMenu :: getKey).collect(toList()));
            }
        }
        dataOverviewVos.forEach(dataOverviewVo -> {
            dataOverviewVo.getBusTargets().forEach(target -> {
                if (shopDataResVos.size() < PC_MAX_DATA_OVERVIEW_SIZE) {
                    ShopDataResVo dataResVo = new ShopDataResVo();
                    dataResVo.setId(target.getId());
                    dataResVo.setProductId(dataOverviewVo.getProductId());
                    dataResVo.setName(target.getTargetName());
                    dataResVo.setKey(target.getKey());
                    Object val = dataMap.get(target.getKey());
                    Object value = 0d;
                    if (Objects.nonNull(val)) {
                        if (val.toString().endsWith("%")) {
                            value = val.toString();
                        } else {
                            try {
                                value = Double.valueOf(val.toString());
                            } catch (NumberFormatException e) {
                                log.info("指标解析异常,异常数据{},指标value:{}",target.toString(),dataMap.get(target.getKey()));
                            }
                        }
                    }
                    dataResVo.setValue(value);
                    if (CollectionUtils.isEmpty(keyList) || keyList.contains(target.getKey())) {
                        shopDataResVos.add(dataResVo);
                    }
                }
            });
        });
        List<ShopDataResVo> resVos = sortData(shopDataResVos,keyList);
        return SoaTransformUtil.success(resVos);
    }


    private List<ShopDataResVo> sortData(List<ShopDataResVo> shopDataResVos,List<String> keyList){
        List<ShopDataResVo> resultList = new ArrayList<>();
        Map<String, ShopDataResVo> dataResMap = shopDataResVos.stream()
                .collect(Collectors.toMap(ShopDataResVo::getKey, Function.identity()));
        if (CollectionUtils.isNotEmpty(keyList)) {
            for (String key : keyList) {
                //权限变更二次过滤
                if (Objects.nonNull(dataResMap.get(key))) {
                    resultList.add(dataResMap.get(key));
                }
            }
            return resultList;
        }
        shopDataResVos.sort(Comparator.comparing(o -> Objects.nonNull(o.getProductId()) ? o.getProductId():Integer.MAX_VALUE));
        return shopDataResVos;

    }

    /**
     * 根据产品附属功能过滤指标
     * @param overviewVos
     * @return
     */
    @Override
    public List<DataOverviewVo> filterByAttachFunction(List<DataOverviewVo> overviewVos,Map<Long, Long> productMap){
        Map<Long, Object> attachFunctionMap = productFacade
                .batchQueryAttachFunctionByKey(Lists.newArrayList(productMap.values()), auxiliaryFunctionKey);
        List<DataOverviewVo> dataOverviewVoList = new ArrayList<>();
        for (DataOverviewVo overviewVo : overviewVos) {
            if (Objects.isNull(overviewVo.getProductId())) {
                dataOverviewVoList.add(overviewVo);
                continue;
            }
            Long instanceId = productMap.get(overviewVo.getProductId());
            if (!attachFunctionMap.containsKey(instanceId) || Objects.isNull(attachFunctionMap.get(instanceId))) {
                continue;
            }
            List<String> keyList = JSONArray.parseArray(attachFunctionMap.get(instanceId).toString()).toJavaList(String.class);
            if (CollectionUtils.isEmpty(keyList)) {
                continue;
            }
            List<BusTargeDto> busTargets = overviewVo.getBusTargets();
            List<BusTargeDto> targetList = busTargets.stream().filter(target -> keyList.contains(target.getKey())).collect(toList());
            if (CollectionUtils.isNotEmpty(targetList)) {
                overviewVo.setBusTargets(targetList);
                dataOverviewVoList.add(overviewVo);
            }
        }
        return dataOverviewVoList;
    }

    /**
     * 查询指标对应的数据
     * 一个productId对应一组指标以及一个url
     * 节点开通的产品对应多组指标数据,入参为多个productId、productInstanceId、url
     *
     * @param reqDto
     * @return
     */
    @Override
    public Map<String,Object> getDataOverview(GetDataOverviewDto reqDto){
        Map<String,Object> resultMap = new HashMap<>();
        LocalDateTime today_start = LocalDateTime.of(LocalDate.now(), LocalTime.MIN);
        LocalDateTime today_end = LocalDateTime.of(LocalDate.now(), LocalTime.MAX);
        DateTimeFormatter time = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String todayStart = time.format(today_start);
        String todayEnd = time.format(today_end);
        ForkJoinPool forkJoinPool = new ForkJoinPool(12);
        try {
            forkJoinPool.submit(()->{
                reqDto.getQueryTargetDtos().parallelStream().forEach(targetDto ->{
                    Map<String,Object> map = new HashMap();
                    map.put("bosId",reqDto.getBosId());
                    map.put("vid",reqDto.getVid());
                    map.put("vidType",reqDto.getVidType());
                    map.put("path",reqDto.getVidPath());
                    map.put("date",LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    map.put("dd",LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    map.put("guiderWid",reqDto.getGuiderWid());
                    map.put("guiderId",reqDto.getGuiderId());
                    map.put("productId",targetDto.getProductId());
                    map.put("productInstanceId",targetDto.getProductInstanceId());
                    map.put("occurStartTime",todayStart);
                    map.put("occurEndTime",todayEnd);
                    HttpResp<ResponseVO> resp = apiUtil.postApi(targetDto.getUrl(), appId, targetDto.getAppKey(), targetDto.getAppSecret(),
                            map, new TypeReference<HttpResp<ResponseVO>>() {});
                    //部分移动端未加此参数,暂时放开
               /* if (!MerchantErrorCode.HTTP_SUCCESS.getErrorCode().equals(resp.getErrcode())) {
                    ExceptionUtils.checkState(false,resp.getErrmsg());
                }*/
                    if (Objects.nonNull(resp.getData()) && CollectionUtils.isNotEmpty(resp.getData().getList())) {
                        Map<String,Object> resMap = resp.getData().getList().get(0);
                        resMap.entrySet().forEach(entry -> {
                            if (targetDto.getKeyList().contains(entry.getKey())) {
                                Object value = entry.getValue();
                                if (Objects.isNull(value)) {
                                    value = 0;
                                }
                                resultMap.put(entry.getKey(),value);
                            }
                        });
                    } else {
                        targetDto.getKeyList().forEach(key ->{
                            resultMap.put(key,0);
                        });
                    }
                });
            }).get();
        } catch (Exception e) {
            log.error("Failed to getDataOverview reqDto={} e={}", JSON.toJSONString(reqDto),e.getMessage());
        } finally {
            forkJoinPool.shutdown();
        }
        return resultMap;
    }

    @Override
    public HttpResponse<List<ShopDataTargetResVo>> getShopDataTarget(GetShopDataTargetReqVo reqVo) {
        Long bosId = reqVo.getBosId();
        Long wid = reqVo.getWid();
        RootNode rootNode = structureFacade.getRootNode(bosId);
        Long rootVid = reqVo.getVid();
        Integer rootVidType = reqVo.getVidType();
        if (Objects.nonNull(rootNode)) {
            rootVid = rootNode.getVid();
            rootVidType = rootNode.getVidType();
        } else {
            return SoaTransformUtil.success(new ArrayList<>());
        }
        //融合节点时查询所有权限
        List<ProductInstanceFlowDTO> productInstanceList;
        if (Constants.DEFAULT_VID.equals(reqVo.getVid()) ||
                (Objects.nonNull(rootNode) && rootNode.getVid().equals(reqVo.getVid()))) {
            productInstanceList = productFacade.getBosProductInstanceId(reqVo.getBosId());
        } else {
            productInstanceList = productFacade.getVidProductInstanceId(reqVo.getVid());
        }
        if (CollectionUtils.isEmpty(productInstanceList)) {
            SoaTransformUtil.success(Collections.EMPTY_LIST);
        }
        List<ProductInstanceIdShowDTO> productInstanceIdShowDTOS = new ArrayList<>();
        //数据指标
        List<DataOverviewVo> dataOverviewVos = getTargetList(reqVo.getBosId(),ShopDataChannelTypeEnum.PC.getChannelType(),reqVo.getVidType());
        //根据导购角色过滤
        final Long guiderVid = DEFAULT_VID.equals(reqVo.getVid()) ? rootVid : reqVo.getVid();
        dataOverviewVos = filterByGuide(bosId,guiderVid,wid,dataOverviewVos);
        //数据指标过滤产品实例
        List<Long> productIds = dataOverviewVos.stream().map(DataOverviewVo::getProductId).collect(toList());
        //(缩小查询范围)
        List<Long> productInstance = productInstanceList.stream()
                .filter(instance -> productIds.contains(instance.getProductId()))
                .map(ProductInstanceFlowDTO::getProductInstanceId)
                .collect(toList());
        if (Constants.DEFAULT_VID.equals(reqVo.getVid()) ||
                (Objects.nonNull(rootNode) && rootNode.getVid().equals(reqVo.getVid()))) {
            productInstanceIdShowDTOS.addAll(privilegeFacade.queryProductInstanceIdShow(wid,bosId,
                    DEFAULT_VID,DEFAULT_VID_TYPE,productInstance));
            productInstanceIdShowDTOS.addAll(privilegeFacade.queryProductInstanceIdShow(wid,bosId,
                    rootVid,rootVidType,productInstance));
        } else {
            productInstanceIdShowDTOS.addAll(privilegeFacade.queryProductInstanceIdShow(wid,bosId,
                    reqVo.getVid(),reqVo.getVidType(),productInstance));
        }
        List<Long> showInstance = productInstanceIdShowDTOS.stream()
                .filter(ProductInstanceIdShowDTO::getShow)
                .map(ProductInstanceIdShowDTO::getProductInstanceId).distinct().collect(toList());
        if (CollectionUtils.isEmpty(showInstance)) {
            SoaTransformUtil.success(Collections.EMPTY_LIST);
        }
        Map<Long, Long> productMap = productInstanceList.stream()
                .filter(instance -> showInstance.contains(instance.getProductInstanceId()))
                .collect(Collectors.toMap(ProductInstanceFlowDTO::getProductId,ProductInstanceFlowDTO::getProductInstanceId));
        //节点可见数据指标
        dataOverviewVos = dataOverviewVos.stream()
                .filter(vo -> Objects.isNull(vo.getProductId()) || productMap.keySet().contains(vo.getProductId()))
                .collect(toList());
        //产品版本附属功能过滤
        dataOverviewVos = filterByAttachFunction(dataOverviewVos,productMap);
        dataOverviewVos.forEach(vo -> {
            //导购特殊分组
            if (SALES_PRODUCT_ID.equals(vo.getProductId()) && Boolean.TRUE.equals(vo.getPrivateGroup())) {
                Long productInstanceId = productMap.get(vo.getProductId());
                String guiderId = getGuiderId(bosId,wid,guiderVid,productInstanceId);
                if(StringUtils.isNotBlank(guiderId)) {
                    String groupName = getGuideGroupName(bosId,guiderVid,wid,guiderId);
                    vo.setGroupName(groupName);
                }
            }
        });
        Map<ShopDataTargetResVo, List<DataOverviewVo>> targetMap = dataOverviewVos.stream()
                .collect(Collectors.groupingBy(o -> {
                    ShopDataTargetResVo resVo = new ShopDataTargetResVo();
                    resVo.setProductId(o.getProductId());
                    resVo.setProductName(o.getProductName());
                    return resVo;
                }));
        List<ShopDataTargetResVo> targetResVos = new ArrayList<>();
        targetMap.forEach((res,overViewList)->{
            res.setProductInstanceId(productMap.get(res.getProductId()));
            Map<TargetGroupDto, List<DataOverviewVo>> groupMap = overViewList.stream()
                    .collect(Collectors.groupingBy(o -> {
                        TargetGroupDto groupDto = new TargetGroupDto();
                        groupDto.setGroupId(o.getGroupId());
                        groupDto.setGroupName(o.getGroupName());
                        return groupDto;
                    }));
            List<TargetGroupDto> targetGroupList = new ArrayList<>();
            groupMap.forEach((group,targetList) ->{
                List<BusTargeDto> targeDtos = targetList.stream().map(DataOverviewVo::getBusTargets)
                        .flatMap(Collection::stream).collect(toList());
                List<TargetDto> targetDtos = BeanCopyUtils.copy(targeDtos, TargetDto.class);
                group.setTargetDtoList(targetDtos);
                targetGroupList.add(group);
            });
            res.setTargetGroupList(targetGroupList);
            targetResVos.add(res);
        });
        targetResVos.sort(Comparator.comparing(o -> Objects.nonNull(o.getProductId()) ? o.getProductId():Integer.MAX_VALUE));
        return SoaTransformUtil.success(targetResVos);
    }

    /**
     * 根据wid vid 换取导购guiderId
     * @param bosId
     * @param wid
     * @param vid
     * @param productInstanceId
     * @return
     */
    @Override
    public String getGuiderId(Long bosId,Long wid,Long vid,Long productInstanceId) {
        GetSingleGuiderByWidAndVidReqDTO reqDTO = new GetSingleGuiderByWidAndVidReqDTO();
        reqDTO.setGuiderWid(wid);
        reqDTO.setGuiderVid(vid);
        BasicInfo basicInfo = new BasicInfo();
        basicInfo.setBosId(bosId);
        basicInfo.setVid(vid);
        basicInfo.setProductId(SALES_PRODUCT_ID);
        basicInfo.setProductInstanceId(productInstanceId);
        reqDTO.setBasicInfo(basicInfo);
        SoaResponse<GuiderDTO, Void> soaResponse = guideGuiderExportService.getGuiderByWidAndVid(reqDTO);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(soaResponse), soaResponse);
        if (Objects.isNull(soaResponse.getResponseVo())) {
            return null;
        }
        return soaResponse.getResponseVo().getGuiderId();
    }

    /**
     * 数据指标根据导购员角色过滤
     * @param bosId
     * @param vid
     * @param wid
     * @param dataOverviewVos
     * @return
     */
    @Override
    public List<DataOverviewVo> filterByGuide(Long bosId,Long vid,Long wid,List<DataOverviewVo> dataOverviewVos){
        Boolean isFilter = dataOverviewVos.stream().anyMatch(vo -> Boolean.TRUE.equals(vo.getPrivateGroup()));
        if (isFilter) {
            QueryAccountInfoRequest request = new QueryAccountInfoRequest();
            request.setBosId(bosId);
            request.setWid(wid);
            request.setVid(vid);
            SoaResponse<AccountInfoDetailDTO, Void> soaResponse = accountInfoExportService.queryAccountInfo(request);
            GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse),MpBizCode.build(soaResponse));
            AccountInfoDetailDTO responseVo = soaResponse.getResponseVo();
            final String roleCode = "DG007";
            if (Objects.isNull(responseVo) || CollectionUtils.isEmpty(responseVo.getRoleList())){
                return dataOverviewVos.stream().filter(vo -> !Boolean.TRUE.equals(vo.getPrivateGroup())).collect(toList());
            }
            List<AccountInfoDetailDTO.RoleInfo> guiderRoleList = responseVo.getRoleList().stream().filter(roleInfo -> {
                String extendAttr = roleInfo.getExtendAttr();
                if (StringUtils.isBlank(extendAttr)) {
                    return false;
                }
                Map<String, String> extendMap;
                try {
                    extendMap = JSON.parseObject(extendAttr, Map.class);
                } catch (Exception e) {
                    // 转换异常，当作不存在未匹配到
                    return false;
                }
                return roleCode.equals(extendMap.get("roleCode"));
            }).collect(toList());
            //无导购角色
            if (CollectionUtils.isEmpty(guiderRoleList)) {
                return dataOverviewVos.stream().filter(vo -> !Boolean.TRUE.equals(vo.getPrivateGroup())).collect(toList());
            } else {
                //只要有导购角色则只展示私有指标
                return dataOverviewVos.stream().filter(vo -> Boolean.TRUE.equals(vo.getPrivateGroup())).collect(toList());
            }
//            //只有导购角色则只展示私有指标
//            if (guiderRoleList.size() == responseVo.getRoleList().size()) {
//                return dataOverviewVos.stream().filter(vo -> !Constants.SALES_PRODUCT_ID.equals(vo.getProductId()) ||
//                        Boolean.TRUE.equals(vo.getPrivateGroup())).collect(toList());
//            }
        }
        return dataOverviewVos;
    }

    /**
     * 获取导购组名
     * @return
     */
    @Override
    public String getGuideGroupName(Long bosId,Long vid,Long wid,String guiderId){
        Map<String,Object> map = new HashMap<>();
        map.put("bosId",bosId);
        map.put("vid",vid);
        map.put("guiderWid",wid);
        map.put("guiderId",guiderId);
        HttpResp<ResponseVO> resp = apiUtil.postApi(groupNameUrl, appId, groupNameKey, groupNameSecret,
                map, new TypeReference<HttpResp<ResponseVO>>() {});
        if (!MerchantErrorCode.HTTP_SUCCESS.getErrorCode().equals(resp.getErrcode())) {
            ExceptionUtils.checkState(false,resp.getErrmsg());
        }
        String groupName = "业绩周期";
        if (Objects.nonNull(resp.getData()) && CollectionUtils.isNotEmpty(resp.getData().getList())) {
            Map<String, Object> resMap = resp.getData().getList().get(0);
            groupName = resMap.get(datakey).toString();
        }
        return groupName;
    }

    @Override
    public List<DataOverviewVo> getTargetList(Long bosId,Integer channelType,Integer vidType) {
        FormInstancePageQuery query = new FormInstancePageQuery();
        query.setFormKey(targetConfigKey);
        query.setPageNumber(Constants.DEFAULT_FIRST_PAGE_NUM);
        query.setPageSize(Constants.ONE_HUNDRED_PAGE_SIZE);
        query.setBosId(bosId);
        SoaResponse<BasePageWrapper<FormInstanceDetailDTO>, Void> soaResponse = formInstanceAbility.pageQueryValidFormInstance(query);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        List<FormInstanceDetailDTO> items = soaResponse.getResponseVo().getItems();
        List<TargetInfoDto> targetDtos = items.stream()
                .map(detailDTO -> {
                    TargetInfoDto targetInfoDto = JSON.parseObject(JSON.toJSONString(detailDTO.getInstanceValue()), new TypeReference<TargetInfoDto>() {});
                    targetInfoDto.setId(detailDTO.getId());
                    return targetInfoDto;
                }).collect(toList());

        Map<DataOverviewVo, List<TargetInfoDto>> dataOverviewVoListMap = targetDtos.stream()
                .filter(target -> ShopDataChannelTypeEnum.ALL.getChannelType().equals(target.getChannelType()) || channelType.equals(target.getChannelType()))
                .filter(target -> StringUtils.isNotBlank(target.getAdapVidType()) && Lists.newArrayList(target
                        .getAdapVidType().split(",")).contains(vidType.toString()))
                .collect(Collectors.groupingBy(target -> BeanCopyUtils.copy(target, DataOverviewVo.class)));
        List<DataOverviewVo> dataOverviewVos = new ArrayList<>();
        dataOverviewVoListMap.forEach((overview,targets) ->{
            List<BusTargeDto> busTargeDtos = BeanCopyUtils.copy(targets, BusTargeDto.class);
            overview.setBusTargets(busTargeDtos);
            dataOverviewVos.add(overview);
        });
        return dataOverviewVos;
    }

    @Override
    public Map<String, Object> queryDataMap(Long bosId, Long vid, Integer vidType,String vidPath, Long wid,
            List<DataOverviewVo> dataOverviewVos,Map<Long, Long> productMap) {
        GetDataOverviewDto reqDto = new GetDataOverviewDto();
        reqDto.setBosId(bosId);
        reqDto.setVid(vid);
        reqDto.setVidType(vidType);
        reqDto.setVidPath(vidPath);
        List<queryTargetDto> queryTargetDtos = new ArrayList<>();
        dataOverviewVos.forEach(overView -> {
            List<String> keyList = overView.getBusTargets().stream().map(BusTargeDto::getKey).collect(toList());
            queryTargetDtos.add(new queryTargetDto(overView.getProductId(),
                    productMap.get(overView.getProductId()), overView.getUrl(),overView.getAppKey(),
                    overView.getAppSecret(),keyList));
            if (SALES_PRODUCT_ID.equals(overView.getProductId()) && Boolean.TRUE.equals(overView.getPrivateGroup())) {
                String guiderId = getGuiderId(bosId,wid,vid, productMap.get(overView.getProductId()));
                reqDto.setGuiderWid(wid);
                reqDto.setGuiderId(guiderId);
            }
        });
        reqDto.setQueryTargetDtos(queryTargetDtos);
        Map<String, Object> dataMap = getDataOverview(reqDto);
        return dataMap;
    }

}
