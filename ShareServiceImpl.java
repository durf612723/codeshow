package com.weimob.mp.merchant.backstage.server.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.PageUtil;
import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weimob.crm.resource.transfer.soa.dto.NoneError;
import com.weimob.crm.resource.transfer.soa.dto.request.CheckCustomerSourceRequest;
import com.weimob.crm.resource.transfer.soa.dto.response.CheckCustomerSourceResponse;
import com.weimob.crm.resource.transfer.soa.service.ResourceTransitRemoteService;
import com.weimob.dep.crm.rpc.dubbo.common.model.ErrInfo;
import com.weimob.mp.merchant.ability.ProductInstanceContractQueryAbility;
import com.weimob.mp.merchant.ability.dto.AttachedFunctionDTO;
import com.weimob.mp.merchant.ability.dto.MenuDTO;
import com.weimob.mp.merchant.ability.request.query.AttachedFunctionBatchQuery;
import com.weimob.mp.merchant.ability.request.query.AttachedFunctionQuery;
import com.weimob.mp.merchant.api.dto.node.AddressDTO;
import com.weimob.mp.merchant.api.product.ProductFlowQueryApi;
import com.weimob.mp.merchant.api.product.dto.ProductActivationFlowDTO;
import com.weimob.mp.merchant.api.product.dto.ProductInstanceFlowDTO;
import com.weimob.mp.merchant.api.product.dto.VidTypeDTO;
import com.weimob.mp.merchant.api.product.request.query.ProductInstanceDetailFlowBatchQuery;
import com.weimob.mp.merchant.api.product.request.query.ProductInstanceDetailFlowQuery;
import com.weimob.mp.merchant.authbase.ability.SubjectRoleAuthService;
import com.weimob.mp.merchant.authbase.ability.domain.role.RoleInfoADTO;
import com.weimob.mp.merchant.authbase.ability.model.rep.SubjectRoleRelationResponse;
import com.weimob.mp.merchant.backstage.api.domain.dto.*;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.share.*;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.shop.CommonKeyReqDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.structure.node.LastActiveNodeRecordDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.ProductVersionDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.product.*;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.product.ProductVersionListVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.product.productListVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.share.*;
import com.weimob.mp.merchant.backstage.common.exception.PageReachedUpperLimitException;
import com.weimob.mp.merchant.backstage.common.exception.ServerException;
import com.weimob.mp.merchant.backstage.server.config.WhileLimitCounterConfig;
import com.weimob.mp.merchant.backstage.server.constant.Constants;
import com.weimob.mp.merchant.backstage.server.constant.MainContentProperties;
import com.weimob.mp.merchant.backstage.server.constant.MpBizCode;
import com.weimob.mp.merchant.backstage.server.constant.SubscribeSourceProperties;
import com.weimob.mp.merchant.backstage.server.dto.DictInfo;
import com.weimob.mp.merchant.backstage.server.dto.QueryPrivilegeTreeDto;
import com.weimob.mp.merchant.backstage.server.enums.*;
import com.weimob.mp.merchant.backstage.server.facade.PrivilegeFacade;
import com.weimob.mp.merchant.backstage.server.facade.ProductFacade;
import com.weimob.mp.merchant.backstage.server.facade.StructureFacade;
import com.weimob.mp.merchant.backstage.server.integration.privilege.RoleInfoInfoIntegrationService;
import com.weimob.mp.merchant.backstage.server.integration.productcenter.ProductCenterIntegrationService;
import com.weimob.mp.merchant.backstage.server.service.account.AccountService;
import com.weimob.mp.merchant.backstage.server.service.AsyncService;
import com.weimob.mp.merchant.backstage.server.service.ShareService;
import com.weimob.mp.merchant.backstage.server.service.cache.MenusMultiCacheManager;
import com.weimob.mp.merchant.backstage.server.util.*;
import com.weimob.mp.merchant.common.enums.PublicAuxiliaryFunctionEnum;
import com.weimob.mp.merchant.common.enums.SceneItemValueEnum;
import com.weimob.mp.merchant.common.enums.VidTypeEnum;
import com.weimob.mp.merchant.common.util.BeanCopyUtils;
import com.weimob.mp.merchant.common.util.KeyGenerater;
import com.weimob.mp.merchant.privilege.ability.*;
import com.weimob.mp.merchant.privilege.ability.domain.dto.Page;
import com.weimob.mp.merchant.privilege.ability.domain.dto.SceneItemDTO;
import com.weimob.mp.merchant.privilege.ability.domain.dto.request.*;
import com.weimob.mp.merchant.privilege.ability.domain.dto.response.*;
import com.weimob.mp.merchant.privilege.ability.domain.enums.BizType;
import com.weimob.mp.merchant.privilege.ability.domain.enums.RoleTypeEnum;
import com.weimob.mp.merchant.product.enums.MenuTypeEnum;
import com.weimob.mp.merchant.product.types.ProductInstanceId;
import com.weimob.mp.merchant.product.types.contract.MenuItem;
import com.weimob.mp.merchant.productcenter.ability.api.ProductRoleAbility;
import com.weimob.mp.merchant.productcenter.ability.api.TopBarEntryAbility;
import com.weimob.mp.merchant.productcenter.ability.dto.ProductRoleDTO;
import com.weimob.mp.merchant.productcenter.ability.dto.ProductRoleVidTypeDTO;
import com.weimob.mp.merchant.productcenter.ability.dto.TopBarEntryDTO;
import com.weimob.mp.merchant.productcenter.ability.request.ProductRoleProductIdQuery;
import com.weimob.mp.merchant.productcenter.ability.request.ProductRoleVidTypeQuery;
import com.weimob.mp.merchant.productcenter.ability.request.QueryTopBarEntryListRequest;
import com.weimob.mp.merchant.productcenter.common.enums.AppTypeEnum;
import com.weimob.mp.merchant.productcenter.common.enums.topbar.TopBarEntryType;
import com.weimob.mp.merchant.redisservice.MPMerchantRedisService;
import com.weimob.mp.merchantstructure.ability.BosExportService;
import com.weimob.mp.merchantstructure.ability.MerchantStructureExportService;
import com.weimob.mp.merchantstructure.ability.NodeInfoExportService;
import com.weimob.mp.merchantstructure.ability.NodeSearchExportService;
import com.weimob.mp.merchantstructure.ability.domain.request.QueryChildCountReqDto;
import com.weimob.mp.merchantstructure.ability.domain.request.QueryVidExtInfoDto;
import com.weimob.mp.merchantstructure.ability.domain.request.QueryVidInfoRequestDto;
import com.weimob.mp.merchantstructure.ability.domain.request.bos.GetVidTypeByBosReqDTO;
import com.weimob.mp.merchantstructure.ability.domain.request.businessGroup.FindAllGroupIdByBosIdAndNameReqDTO;
import com.weimob.mp.merchantstructure.ability.domain.request.search.*;
import com.weimob.mp.merchantstructure.ability.domain.response.ParentToChildCountRespDto;
import com.weimob.mp.merchantstructure.ability.domain.response.QueryVidInfoResponseDto;
import com.weimob.mp.merchantstructure.ability.domain.response.bos.GetVidTypeByBosRespDTO;
import com.weimob.mp.merchantstructure.ability.domain.response.businessGroup.BusinessGroupInfoDTO;
import com.weimob.mp.merchantstructure.ability.domain.response.businessGroup.FindAllGroupIdByBosIdAndNameRespDTO;
import com.weimob.mp.merchantstructure.ability.domain.response.search.VidPageRespDTO;
import com.weimob.mp.merchantstructure.common.domain.common.BusinessOsDto;
import com.weimob.mp.merchantstructure.common.domain.common.NodeBaseInfoDto;
import com.weimob.mp.merchantstructure.common.domain.common.search.VidPageInfoDTO;
import com.weimob.mp.merchantstructure.common.enums.NodeSearchParentNodeEnum;
import com.weimob.mp.merchantstructure.common.enums.VidPageOrderFiledEnum;
import com.weimob.mp.merchantstructure.common.enums.VidStatusEnum;
import com.weimob.mp.passport.account.ability.response.PhoneZoneInfoResponse;
import com.weimob.mp.passport.account.ability.tob.BusinessAccountZonesAbility;
import com.weimob.nbiz.cluecenter.biz.facade.ClueFacade;
import com.weimob.nbiz.cluecenter.biz.facade.dto.clue.request.ContactReqDTO;
import com.weimob.nbiz.cluecenter.biz.facade.dto.clue.request.CreateSingleClueDTO;
import com.weimob.nbiz.cluecenter.biz.facade.dto.clue.request.PurposeReqDTO;
import com.weimob.nbiz.cluecenter.biz.facade.dto.clue.request.SourceDTO;
import com.weimob.nbiz.cluecenter.biz.facade.dto.clue.response.CreateSingleClueResponseDTO;
import com.weimob.nbiz.cluecenter.common.constant.enums.SourceTypeEnum;
import com.weimob.nbiz.cluecenter.common.constant.enums.UrgentTypeEnum;
import com.weimob.nbiz.goodscenter.mp.ability.api.ProductVersionManagerAbility;
import com.weimob.nbiz.goodscenter.mp.ability.dto.ProductVersionDTO;
import com.weimob.nbiz.goodscenter.mp.ability.request.ProductVersionQueryReqDTO;
import com.weimob.saas.common.spf.core.lib.aop.ErrorWrapper;
import com.weimob.saas.common.spf.core.lib.utils.GEPreconditions;
import com.weimob.saas.common.spf.core.lib.utils.SoaRespUtils;
import com.weimob.saas.mall.common.response.HttpResponse;
import com.weimob.soa.common.response.SoaResponse;
import com.weimob.zipkin.ZipkinContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.weimob.mp.merchant.backstage.server.constant.Constants.*;
import static com.weimob.mp.merchant.backstage.server.service.impl.ProductServiceImpl.objToList;
import static com.weimob.mp.merchant.backstage.server.util.ShaUtil.SHA1WithUpper;
import static com.weimob.mp.merchant.common.enums.PublicAuxiliaryFunctionEnum.ADAPTER_VID_TYPE_OPTION;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Service
public class ShareServiceImpl implements ShareService {

    @Value("${key.udesk}")
    private String imUserKey;
    @Reference
    private IntegrationExportService integrationExportService;
    @Resource
    private MPMerchantRedisService mpMerchantRedisService;
    @Autowired
    private MenusMultiCacheManager menusMultiCacheManager;
    @Resource
    private PrivilegeExportService privilegeExportService;
    @Reference
    private ProductFlowQueryApi productFlowApi;
    @Resource
    private TopBarEntryAbility topBarEntryAbility;
    @Resource
    private DataPrivilegeExportService dataPrivilegeExportService;
    @Resource
    private ProductVersionManagerAbility productVersionManagerAbility;
    @Resource
    private StructureFacade structureFacade;
    @Resource
    private PrivilegeFacade privilegeFacade;
    @Resource
    private ClueFacade clueFacade;
    @Resource
    private ProductFacade productFacade;
    /**
     * 产品管理菜单id
     */
    @Value("${productManageMenuId:-1}")
    private Long productManageMenuId;
    @Reference
    private ProductFlowQueryApi productFlowQueryApi;
    @Resource
    private AccountService accountService;
    @Autowired
    private AsyncService asyncService;
    @Reference
    private BusinessAccountZonesAbility accountZonesAbility;
    @Reference
    private ProductInstanceContractQueryAbility productInstanceContractQueryAbility;
    @Reference
    private RoleInfoExportService roleInfoExportService;

    @Resource
    private RoleInfoInfoIntegrationService roleInfoInfoIntegrationService;
    @Resource
    private ProductCenterIntegrationService productCenterIntegrationService;

    @Autowired
    private SubscribeSourceProperties subscribeSourceProperties;
    @Reference
    private ResourceTransitRemoteService remoteService;
    @Reference
    private BosExportService bosExportService;
    @Reference
    private AccountInfoExportService accountInfoExportService;
    @Resource
    private NodeInfoExportService nodeInfoExportService;
    @Autowired
    private MainContentProperties mainContentProperties;
    @Reference
    private OverPrivilegeExportService overPrivilegeExportService;
    @Reference
    private NodeSearchExportService nodeSearchExportService;
    @Reference
    private MerchantStructureExportService merchantStructureExportService;
    @Reference
    private SubjectRoleAuthService subjectRoleAuthService;
    @Autowired
    private WhileLimitCounterConfig whileLimitCounterConfig;
    @Reference
    private ProductRoleAbility productRoleAbility;

    @Override
    public HttpResponse<List<VidTypeDto>> vidTypeList(VidTypeListReqVo reqVo) {
        Boolean isNeedThisType=reqVo.getIsNeedThisType();
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
        Long rootVid = Objects.nonNull(rootNode) ? rootNode.getVid() : DEFAULT_VID;
        Map<Long ,List<VidTypeDTO>>  mapVidTypes = productFacade.queryMountableNodeTypeListByBosId(reqVo.getBosId(), reqVo.getVid(), reqVo.getVidType(), rootVid);
//        List<VidTypeDTO> vidTypes = mapVidTypes.values().stream().flatMap(Collection::stream).collect(toList());
        Map<Long, List<VidTypeDto>> newMapVidTypes = new HashMap<>();
        mapVidTypes.forEach((k, v) -> {
            List<VidTypeDto> vidTypeList = BeanCopyUtils.copy(v, VidTypeDto.class);
            if (CollectionUtils.isNotEmpty(vidTypeList)) {
                vidTypeList = vidTypeList.stream().filter(type -> !type.getType().equals(reqVo.getVidType())).collect(toList());
            }
            newMapVidTypes.put(k, vidTypeList);
        });
        List<VidTypeDto> vidTypeList = newMapVidTypes.values().stream().flatMap(Collection::stream).collect(toList());
        if (Objects.nonNull(reqVo.getRoleId())) {
            List<RoleInfoADTO> roleInfoADTOList = roleInfoInfoIntegrationService.getRoleInfoListByRoleIds(reqVo.getBosId(), reqVo.getRoleId());
            if (CollectionUtils.isEmpty(roleInfoADTOList)) {
                throw new ServerException("角色不存在");
            }
            RoleInfoADTO roleInfoADTO = roleInfoADTOList.get(0);
            if (Objects.equals(roleInfoADTO.getRoleType(), RoleTypeEnum.SYSTEM_DEFAULT_NORMAL_ROLE.getRoleType()) && StringUtils.isNotBlank(roleInfoADTO.getRoleCode())) {
                List<ProductRoleDTO> productRoleDTOList = productCenterIntegrationService.queryProductRoleInfoList(roleInfoADTO.getRoleCode());
                if (CollectionUtils.isNotEmpty(productRoleDTOList) && Objects.equals(productRoleDTOList.get(0).getUseConstraints(), 2)) {
                    isNeedThisType = false;
                    List<Long> roleProductIdList = productCenterIntegrationService.queryProductRoleProductIdList(null, roleInfoADTO.getRoleCode());
                    // 查询角色下所有资源（跨应用）
                    List<ProductRoleVidTypeDTO> productRoleVidTypeDTOList = new ArrayList<>();
                    roleProductIdList.forEach(productId -> {
                        ProductRoleVidTypeQuery query = ProductRoleVidTypeQuery.builder().productId(productId).roleCode(roleInfoADTO.getRoleCode()).build();
                        List<ProductRoleVidTypeDTO> productRoleList = productCenterIntegrationService.queryProductRoleVidType(query);
                        if (CollectionUtils.isNotEmpty(productRoleList)) {
                            productRoleVidTypeDTOList.addAll(productRoleList);
                        }
                    });
                    Map<Long, List<ProductRoleVidTypeDTO>> sysRoleVidTypeMap = productRoleVidTypeDTOList.stream().collect(Collectors.groupingBy(ProductRoleVidTypeDTO::getProductId));
                    Map<Long, List<VidTypeDto>> vidTypeDtoMap = new HashMap<>();
                    newMapVidTypes.forEach((k, v) -> {
                        List<ProductRoleVidTypeDTO> productRoleVidTypeDTOS=sysRoleVidTypeMap.get(k);
                        if (CollectionUtils.isNotEmpty(productRoleVidTypeDTOS)){
                            List<Integer> sysRoleVidTypeList = sysRoleVidTypeMap.get(k).stream().map(ProductRoleVidTypeDTO::getVidType).distinct().collect(toList());
                            List<VidTypeDto> vidTypeDtoList = v.stream().filter(k1 -> sysRoleVidTypeList.contains(k1.getType())).collect(toList());
                            vidTypeDtoMap.put(k, vidTypeDtoList);
                        }
                    });
                    vidTypeList =  vidTypeDtoMap.values().stream().flatMap(Collection::stream).collect(toList());
                }
            }
        }
        //是否需要当前节点
        if (Objects.equals(isNeedThisType, Boolean.TRUE)) {
            vidTypeList.add(new VidTypeDto(reqVo.getVidType(), VidTypeEnum.enumByCode(reqVo.getVidType()).getDesc()));
        }
        vidTypeList = vidTypeList.stream().map(type -> {
            if (DEFAULT_VID_TYPE.equals(type.getType()) ||
                    (Objects.nonNull(rootNode) && rootNode.getVidType().equals(type.getType()))) {
                type.setType(rootNode.getVidType());
                type.setName(mainContentProperties.getVidTypeName(reqVo.getBosId()));
                type.setIsRootVidType(Boolean.TRUE);
            }
            return type;
        }).filter(type -> Objects.isNull(rootNode) || type.getType() >= rootNode.getVidType()).distinct()
                .sorted(Comparator.comparing(VidTypeDto::getType)).collect(toList());


        //不需要当前节点时,融合节点特殊处理
        if ((!reqVo.getIsNeedThisType() && DEFAULT_VID.equals(reqVo.getVid())) || !reqVo.getIsNeedRootType()) {
            List<Integer> filterType = new ArrayList<>();
            filterType.add(DEFAULT_VID_TYPE);
            filterType.add(Objects.nonNull(rootNode) ? rootNode.getVidType() : DEFAULT_VID_TYPE);
            vidTypeList = vidTypeList.stream().filter(type -> !filterType.contains(type.getType())).collect(toList());
        }
        return SoaTransformUtil.success(vidTypeList);
    }

    @Override
    public HttpResponse<List<VidTypeDto>> vidTypeListForVid(VidTypeListForVidReqVo reqVo) {
        GetVidTypeByBosReqDTO reqDTO = new GetVidTypeByBosReqDTO();
        reqDTO.setBosId(reqVo.getBosId());
        reqDTO.setParentVid(reqVo.getVid());
        reqDTO.setProductInstanceIdList(reqVo.getProductInstanceIdList());
        if (Objects.nonNull(reqVo.getFilterProductInstanceId())) {
            reqDTO.setFilterProductInstanceIdList(Arrays.asList(reqVo.getFilterProductInstanceId()));
        }
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
        //业务组类型查询参数处理
        if (reqVo.getGroupType() != null || reqVo.getGroupId() != null
                || reqVo.getUnrelatedGroupType() != null) {
            SearchBusinessGroupReqDTO groupReqDTO = new SearchBusinessGroupReqDTO();
            if (reqVo.getUnrelatedGroupType() != null && reqVo.getUnrelatedGroupType()) {
                GEPreconditions.checkState(Objects.nonNull(reqVo.getGroupType()), MpBizCode.GROUP_TYPE_ERROR);
                groupReqDTO.setFilter(Boolean.TRUE);
            } else {
                groupReqDTO.setFilter(Boolean.FALSE);
            }
            groupReqDTO.setGroupType(reqVo.getGroupType());
            if (reqVo.getGroupId() != null) {
                groupReqDTO.setGroupIdList(Collections.singletonList(reqVo.getGroupId()));
            }
            reqDTO.setBusinessGroup(groupReqDTO);
        }
        SoaResponse<GetVidTypeByBosRespDTO, Void> vidTypeByBos = bosExportService.getVidTypeByBos(reqDTO);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(vidTypeByBos), MpBizCode.build(vidTypeByBos));
        List<VidTypeDTO> vidTypeList = vidTypeByBos.getResponseVo().getVidTypes().stream()
                .map(type -> new VidTypeDTO(type, VidTypeEnum.enumByCode(type).getDesc())).collect(toList());
        if (Objects.nonNull(reqVo.getVidType()) && !DEFAULT_VID_TYPE.equals(reqVo.getVidType())) {
            vidTypeList.add(new VidTypeDTO(reqVo.getVidType(), VidTypeEnum.enumByCode(reqVo.getVidType()).getDesc()));
        }
        if (CollectionUtils.isNotEmpty(reqVo.getSelectVidTypeList())) {
            vidTypeList = vidTypeList.stream()
                    .filter(type -> reqVo.getSelectVidTypeList().contains(type.getType()))
                    .collect(toList());
        }
        vidTypeList = vidTypeList.stream()
                .map(type -> {
                    if (Objects.nonNull(rootNode) && rootNode.getVidType().equals(type.getType())) {
                        type.setName(mainContentProperties.getVidTypeName(reqVo.getBosId()));
                    }
                    return type;
                })
                .distinct().sorted(Comparator.comparing(VidTypeDTO::getType)).collect(toList());
        List<VidTypeDto> resList = BeanCopyUtils.copy(vidTypeList, VidTypeDto.class);
        return SoaTransformUtil.success(resList);
    }

    @Override
    public HttpResponse<List<VidInfoDto>> queryVidsByWid(QueryVidsByWidReqVo reqVo) {
        // 节点融合
        BusinessOsDto bos = Objects.requireNonNull(structureFacade.getBosBaseInfo(reqVo.getBosId()));
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());

        IntegrationQueryRequest queryRequest = BeanCopyUtils.copy(reqVo, IntegrationQueryRequest.class);
        SoaResponse<QueryVidInfoPageDTO, Void> soaResponse = integrationExportService
                .querySelectableVids(queryRequest);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        List<VidInfoDTO> vidInfoDTOS = soaResponse.getResponseVo().getList();
        List<VidInfoDto> resList = new ArrayList<>();
        if (CollectionUtils.isEmpty(vidInfoDTOS)) {
            return SoaTransformUtil.success(resList);
        }
        List<Long> parentVidList = vidInfoDTOS.stream().map(VidInfoDTO::getParentVid)
                .filter(id -> Objects.nonNull(id) && !Constants.DEFAULT_VID.equals(id))
                .distinct()
                .collect(toList());
        Map<Long, NodeBaseInfoDto> baseInfoDtoMap = structureFacade.selectVidName(parentVidList);
        String defaultName = null;
        for (VidInfoDTO vidInfoDTO : vidInfoDTOS) {
            //融合节点判断
            if (vidInfoDTO.getVid() == null || Objects.equals(Constants.DEFAULT_VID, vidInfoDTO.getVid())) {
                vidInfoDTO.setVid(Constants.DEFAULT_VID);
                defaultName = structureFacade.getDefaultBosName(reqVo.getBosId());
                vidInfoDTO.setVidName(defaultName);
                vidInfoDTO.setVidTypes(Arrays.asList(DEFAULT_VID_TYPE));
            }
            VidInfoDto vidInfoDto = BeanCopyUtils.copy(vidInfoDTO, VidInfoDto.class);
            if (Objects.nonNull(vidInfoDto.getParentVid())) {
                vidInfoDto.setParentVidName(Constants.DEFAULT_VID.equals(vidInfoDto.getParentVid()) ? defaultName :
                        baseInfoDtoMap.get(vidInfoDto.getParentVid()).getVidName());
            }
            vidInfoDto.setVidType(vidInfoDTO.getVidTypes().get(0));

            // 更新名称
            if (rootNode != null && vidInfoDto.getVid().equals(rootNode.getVid())) {
                vidInfoDto.setVidName(bos.getBosName());
            }

            if (Constants.DEFAULT_VID.equals(reqVo.getVid())) {
                resList.add(vidInfoDto);
            } else if (StringUtils.isNotBlank(vidInfoDTO.getVidPath()) && Arrays.asList(vidInfoDTO.getVidPath()
                    .split("-")).contains(reqVo.getVid().toString())) {
                resList.add(vidInfoDto);
            }
        }
        return SoaTransformUtil.success(resList);
    }

    @Override
    public HttpResponse<GetIdsByKeyResVo> getIdsByKey(GetIdsByKeyReqVo reqVo) {
        BaseInfoForKeyRequest baseInfoForKeyRequest = new BaseInfoForKeyRequest();
        baseInfoForKeyRequest.setKey(reqVo.getKey());
        SoaResponse<BaseInfoForKeyDTO, Void> soaResponse = integrationExportService
                .queryInfoByKey(baseInfoForKeyRequest);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        BaseInfoForKeyDTO responseVo = soaResponse.getResponseVo();
        GEPreconditions.checkState(responseVo != null, MpBizCode.GET_IDS_BY_KEY_ERROR);
        GetIdsByKeyResVo resVo = BeanCopyUtils.copy(responseVo, GetIdsByKeyResVo.class);
        //产品实例不为空查询产品信息
        if (!responseVo.getIsStoreId() && !Constants.DEFAULT_PRODUCT_INSTANCE_ID.equals(responseVo.getProductInstanceId())) {
            ProductInstanceDetailFlowQuery query = new ProductInstanceDetailFlowQuery(new ProductInstanceId(resVo.getProductInstanceId()));
            SoaResponse<ProductInstanceFlowDTO, Void> voidSoaResponse = productFlowApi
                    .queryProductInstanceByInstanceId(query);
            GEPreconditions.checkState(
                    SoaRespUtils.isSuccess(voidSoaResponse) && Objects.nonNull(voidSoaResponse.getResponseVo()),
                    MpBizCode.GET_PRODUCT_INSTANCE_ERROR);
            resVo.setProductName(voidSoaResponse.getResponseVo().getProductName());
            resVo.setStatusCode(voidSoaResponse.getResponseVo().getStatusCode());
            resVo.setProductVersionName(voidSoaResponse.getResponseVo().getVersionName());
        }
        //bos鉴权 wid和bosid无对应关系
        QueryAccountInfoListByWidsRequest bosWidRequest = QueryAccountInfoListByWidsRequest.builder()
                .wids(Arrays.asList(reqVo.getWid()))
                .bosId(responseVo.getBosId())
                .build();
        SoaResponse<AccountListPageDTO<AccountBriefInfoDTO>, Void> accountSoaResponse = accountInfoExportService.queryBaseInfoListByWids(bosWidRequest);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(accountSoaResponse) && (CollectionUtils.isNotEmpty(accountSoaResponse
                .getResponseVo().getAccountList())), MpBizCode.BOS_PRIVILEGE_ERROR);
        //bos封禁鉴权
        ExceptionUtils.checkState(Objects.isNull(responseVo.getBosStatus()) || PublicIsOrNotEnum.TRUE.getIntCode()
                .equals(responseVo.getBosStatus()), MpBizCode.BOS_STATUS_ERROR);
        //bos鉴权 wid是店铺下账号,但未被分配任何节点
        IntegrationQueryRequest request = new IntegrationQueryRequest();
        request.setWid(reqVo.getWid());
        request.setBosId(responseVo.getBosId());
        SoaResponse<List<Long>, Void> response = integrationExportService.querySelectableBosIds(request);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(response) && (CollectionUtils.isNotEmpty(response
                .getResponseVo()) && response.getResponseVo().contains(responseVo.getBosId())), MpBizCode.VID_PRIVILEGE_ERROR);
        //vid鉴权 校验账号是否有vid权限
        QuerySubjectRoleRelationRequest vidRequest = QuerySubjectRoleRelationRequest.builder()
                .bosId(responseVo.getBosId())
                .bizId(reqVo.getWid())
                .bizType(BizType.ACCOUNT.ordinal())
                .vid(responseVo.getVid())
                .build();
        SoaResponse<List<SubjectRoleRelationDTO>, Void> voidSoaResponse = accountInfoExportService
                .querySubjectRoleRelation(vidRequest);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(voidSoaResponse), voidSoaResponse);
        if (CollectionUtils.isEmpty(voidSoaResponse.getResponseVo())) {
            resVo.setVidRelation(Boolean.FALSE);
        }
        //校验当前是否为越权
        QueryActivationOverPrivilegeConfigRequest configRequest = QueryActivationOverPrivilegeConfigRequest.builder()
                .bosId(responseVo.getBosId())
                .wid(reqVo.getWid())
                .targetVid(responseVo.getVid())
                .build();
        SoaResponse<ActivationOverPrivilegeConfigDTO, Void> configDTOVoidSoaResponse =
                overPrivilegeExportService.queryActivationOverPrivilegeConfig(configRequest);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(configDTOVoidSoaResponse), configDTOVoidSoaResponse);
        if (Objects.nonNull(configDTOVoidSoaResponse.getResponseVo()) && responseVo.getVid()
                .equals(configDTOVoidSoaResponse.getResponseVo().getTargetVid())) {
            resVo.setOverPrivilegeTime(configDTOVoidSoaResponse.getResponseVo().getExpirationTime().getTime());
            resVo.setVidRelation(Boolean.TRUE);
        }
        //校验vid与productInstanceId开通关系 activeStatus
        if (!Constants.DEFAULT_PRODUCT_INSTANCE_ID.equals(responseVo.getProductInstanceId())) {
            Boolean activeStatus = productFacade
                    .checkProductInstanceForVid(responseVo.getBosId(), responseVo.getVid(), responseVo.getProductId(),
                            responseVo.getProductInstanceId());
            resVo.setActiveStatus(activeStatus);
        }
        //特殊结果集拼接
        if (CollectionUtils.isNotEmpty(responseVo.getVidTypes())) {
            resVo.setVidType(responseVo.getVidTypes().get(0));
            resVo.setVidTypeName(DEFAULT_VID_TYPE.equals(resVo.getVidType()) ? Constants.VIDTYPENAME :
                    VidTypeEnum.enumByCode(resVo.getVidType()).getDesc());
        } else {
            resVo.setVidType(DEFAULT_VID_TYPE);
            resVo.setVidTypes(Arrays.asList(DEFAULT_VID_TYPE));
            resVo.setVidTypeName(Constants.VIDTYPENAME);
        }
        if (Objects.nonNull(resVo.getBosId()) && (resVo.getVid() == null || Objects.equals(Constants.DEFAULT_VID, resVo
                .getVid())) || resVo.getVid().equals(structureFacade.getRootNodeVid(resVo.getBosId()))) {
            String vidName = structureFacade.getDefaultBosName(resVo.getBosId());
            resVo.setVidName(vidName + Constants.FUSE_VIDTYPENAME);
        }
        RootNode rootNode = structureFacade.getRootNode(resVo.getBosId());
        if (Objects.nonNull(rootNode)) {
            if (resVo.getVid().equals(rootNode.getVid())) {
                resVo.setIsRootVid(Boolean.TRUE);
            }
            resVo.setRootVid(rootNode.getVid());
            resVo.setRootVidType(rootNode.getVidType());
        }
        //增加网店状态字段
        if (!DEFAULT_VID.equals(resVo.getVid())) {
            QueryVidInfoRequestDto requestDto = new QueryVidInfoRequestDto();
            requestDto.setVid(resVo.getVid());
            //设置扩展字段请求入参
            try {
                QueryVidExtInfoDto extInfoDto = new QueryVidExtInfoDto();
                extInfoDto.setProductId(DEFAULT_PRODUCT_ID);
                extInfoDto.setProductInstanceId(productFacade.getDefaultProductInstanceId(responseVo.getBosId()));
                extInfoDto.setExtFields(Arrays.asList(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey()));
                requestDto.setVidExtInfos(Arrays.asList(extInfoDto));
                SoaResponse<QueryVidInfoResponseDto, Void> vidInfo = nodeInfoExportService.getVidInfo(requestDto);
                if (Objects.nonNull(vidInfo) && CollectionUtils.isNotEmpty(vidInfo.getResponseVo().getExtInfos())) {
                    Map<String, String> extInfoMap = vidInfo.getResponseVo().getExtInfos().stream()
                            .filter(o -> Constants.DEFAULT_PRODUCT_ID.equals(o.getProductId())).findFirst().get()
                            .getExtInfoMap();
                    String wxOnlineStatus = extInfoMap.get(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey());
                    if (StringUtils.isNotBlank(wxOnlineStatus)) {
                        resVo.setWxOnlineStatus(Integer.parseInt(wxOnlineStatus));
                    }
                }
            } catch (Exception e) {
                log.info("查询网店状态失败,异常信息{}", e);
            }
        }
        // 增加parentVid字段
        resVo.setParentVid(VidPathUtil.findParentVidByParentPath(resVo.getVidPath(), resVo.getVid()));

        return SoaTransformUtil.success(resVo);
    }

    @Override
    public HttpResponse<List<DataPrivilegeInfoDTO>> queryDataByMenuCode(QueryDataByMenuCodeReqVo reqVo) {
        QueryDataByMenuCodeRequest request = QueryDataByMenuCodeRequest.builder()
                .bosId(reqVo.getBosId())
                .menuCode(reqVo.getMenuCode())
                .build();
        SoaResponse<List<DataPrivilegeInfoDTO>, Void> soaResponse = dataPrivilegeExportService
                .queryDataByMenuCode(request);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        return SoaTransformUtil.build(soaResponse);
    }

    @Override
    public HttpResponse<Map<String, List<DictInfo>>> getDictByKey(String key) {
        Map<String, List<DictInfo>> maps = new HashMap<>();
        String[] enmus = key.split(",");
        for (String name : enmus) {
            try {
                Class<? extends Enum> clazz = (Class<Enum<? extends Enum>>) Class
                        .forName(DictPackageEnum.getPackage(name));
                Enum<? extends Enum>[] enums = clazz.getEnumConstants();
                Method[] methods = clazz.getDeclaredMethods();
                List<DictInfo> dictInfos = new ArrayList<>();
                for (Enum<? extends Enum> en : enums) {
                    DictInfo dict = new DictInfo();
                    for (Method method : methods) {
                        if (method.getName().contains("get") && method.getParameterCount() == 0) {
                            Object result = method.invoke(en);
                            if (result instanceof Integer) {
                                dict.setKey(((Integer) result));
                            }
                            if (result instanceof String) {
                                dict.setValue(((String) result));
                            }
                        }
                    }
                    dictInfos.add(dict);
                }
                maps.put(name, dictInfos);
            } catch (Exception e) {
                continue;
            }
        }

        return SoaTransformUtil.success(maps);
    }

    @Override
    public HttpResponse<NodeAttributionTreeResVo> nodeAttributionTree(NodeAttributionTreeReqVo reqVo) {
        if (reqVo.getPageSize() > Constants.MAX_PAGE_SIZE) {
            reqVo.setPageSize(Constants.MAX_PAGE_SIZE);
        }
        QueryMultipleVidPageReqDTO request = new QueryMultipleVidPageReqDTO();
        QueryMultipleVidDetailReqDTO detail = new QueryMultipleVidDetailReqDTO();
        request.setPage(reqVo.getPageNum());
        request.setSize(reqVo.getPageSize());
        request.setBosId(Objects.nonNull(reqVo.getBosId()) ? reqVo.getBosId() : reqVo.getBasicInfo().getBosId());
        List<Integer> vidTypes = reqVo.getVidTypeList().stream().filter(Objects::nonNull).collect(toList());
        detail.setVidTypes(vidTypes);
        detail.setVidStatus(reqVo.getVidStatus());
        if (StringUtils.isNotBlank(reqVo.getVidName())) {
            detail.setVidName(reqVo.getVidName());
        }
        if (CollectionUtils.isNotEmpty(reqVo.getProductIdList())) {
            detail.setProductIdList(reqVo.getProductIdList());
        }
        if (CollectionUtils.isNotEmpty(reqVo.getProductInstanceIdList())) {
            detail.setProductInstanceIdList(reqVo.getProductInstanceIdList());
        }
        if (Objects.nonNull(reqVo.getParentVid()) || CollectionUtils.isNotEmpty(reqVo.getParentVidList())) {
            detail.setParentVid(reqVo.getParentVid());
            detail.setParentVidList(reqVo.getParentVidList());
            detail.setDepthLayer(Boolean.TRUE);
            if (reqVo.getIsContainParent()) {
                request.setResultData(Arrays.asList(NodeSearchParentNodeEnum.PARENT_NODE.getCode()));
            }
        }
        //业务组类型查询参数处理
        if (reqVo.getGroupType() != null || reqVo.getGroupId() != null
                || reqVo.getUnrelatedGroupType() != null) {
            SearchBusinessGroupReqDTO groupReqDTO = new SearchBusinessGroupReqDTO();
            if (reqVo.getUnrelatedGroupType() != null && reqVo.getUnrelatedGroupType()) {
                GEPreconditions.checkState(Objects.nonNull(reqVo.getGroupType()), MpBizCode.GROUP_TYPE_ERROR);
                groupReqDTO.setFilter(Boolean.TRUE);
            } else {
                groupReqDTO.setFilter(Boolean.FALSE);
            }
            groupReqDTO.setGroupType(reqVo.getGroupType());
            if (reqVo.getGroupId() != null) {
                groupReqDTO.setGroupIdList(Collections.singletonList(reqVo.getGroupId()));
            }
            request.setBusinessGroup(groupReqDTO);
        }
        List<VidPageExtMatchReqDTO> extMatchList = new ArrayList<>();
        if (Objects.nonNull(reqVo.getWxOnlineStatus()) && (reqVo.getVidTypeList().contains(VidTypeEnum.BRAND
                .getType()) || reqVo.getVidTypeList().contains(VidTypeEnum.STORE.getType()))) {
            VidPageExtMatchReqDTO wxOnlineStatusReq = new VidPageExtMatchReqDTO();
            wxOnlineStatusReq.setProductId(Constants.DEFAULT_PRODUCT_ID);
            wxOnlineStatusReq.setSettingKey(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey());
            wxOnlineStatusReq.setSettingValues(Arrays.asList(reqVo.getWxOnlineStatus().toString()));
            extMatchList.add(wxOnlineStatusReq);
        }
        detail.setExtMatchList(extMatchList);
        List<QueryMultipleVidDetailReqDTO> details = new ArrayList<>();
        details.add(detail);
        request.setDetails(details);
        List<Long> vidList = null;
        if (reqVo.getQueryAllParentVid() && Objects.nonNull(reqVo.getParentVid())) {
            QueryVidInfoResponseDto vidInfo = structureFacade.getVidInfo(reqVo.getParentVid());
            vidList = vidInfo.getParentPathTree();
        }
        List<NodeTreeInfoDto> nodeTreeInfoDtos = structureFacade.selectNodeList(request, vidList);
        if (Objects.nonNull(reqVo.getIsTree()) && reqVo.getIsTree().equals(PublicIsOrNotEnum.TRUE.getIntCode())) {
            //过滤不在树内的节点
            List<Integer> vidTypeList = reqVo.getVidTypeList();
            if (CollectionUtils.isEmpty(reqVo.getVidTypeList())) {
                for (VidTypeEnum vidTypeEnum : VidTypeEnum.values()) {
                    if (!DEFAULT_VID_TYPE.equals(vidTypeEnum.getType())) {
                        vidTypeList.add(vidTypeEnum.getType());
                    }
                }
            }
            List<Long> vids = nodeTreeInfoDtos.stream().map(NodeTreeInfoDto::getVid).collect(toList());

            for (Integer type : vidTypeList) {
                if (CollectionUtils.isNotEmpty(nodeTreeInfoDtos.stream().filter(dto -> type.equals(dto.getVidType()))
                        .collect(toList()))) {
                    nodeTreeInfoDtos = nodeTreeInfoDtos.stream().filter(node -> vids.contains(node
                            .getParentVid()) || node.getVidType().equals(type)).collect(toList());
                    nodeTreeInfoDtos.forEach(dto -> {
                        if (!vids.contains(dto.getParentVid())) {
                            dto.setRootVid(Constants.DEFAULT_VID);
                            dto.setParentVid(Constants.DEFAULT_VID);
                        }
                    });
                    break;
                }
                continue;
            }
        }
        NodeAttributionTreeResVo resVo = new NodeAttributionTreeResVo(nodeTreeInfoDtos);
        return SoaTransformUtil.success(resVo);
    }

    @Override
    public AttributionSelectResVo<AttributionNodeDto> attributionSelectTree(AttributionSelectReqVo reqVo) {
        QueryVidPageReqDTO request = new QueryVidPageReqDTO();
        request.setPage(reqVo.getPageNum());
        request.setSize(reqVo.getPageSize());
        request.setBosId(reqVo.getBosId());
        List<Integer> vidTypes = reqVo.getVidTypeList().stream().filter(Objects::nonNull).collect(toList());
        request.setVidTypes(vidTypes);
        if (StringUtils.isNotBlank(reqVo.getVidName())) {
            request.setVidName(reqVo.getVidName());
            request.setDepthLayer(Boolean.TRUE);
        }
        if (CollectionUtils.isNotEmpty(reqVo.getProductInstanceIdList())) {
            request.setProductInstanceIdList(reqVo.getProductInstanceIdList());
        }
        List<String> resultData = new ArrayList<>();
        Long parentVid = reqVo.getParentVid();
        if (Objects.isNull(parentVid)) {
            parentVid = structureFacade.getRootNodeVid(reqVo.getBosId());
        }
        request.setParentVid(parentVid);
        if (reqVo.getIsContainParent()) {
            resultData.add(NodeSearchParentNodeEnum.PARENT_NODE.getCode());
        }
        resultData.add(NodeSearchParentNodeEnum.HAS_CHILD.getCode());
        request.setResultData(resultData);
        // 手动设置排序
        VidPageOrderReqDTO vidPageOrderReqDTO = new VidPageOrderReqDTO();
        vidPageOrderReqDTO.setField(VidPageOrderFiledEnum.CREATE_TIME.getField());
        vidPageOrderReqDTO.setDesc(false);
        request.setSorts(Collections.singletonList(vidPageOrderReqDTO));
        SoaResponse<VidPageRespDTO, Void> soaResponse = nodeSearchExportService
                .queryVidPage2(request);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        List<VidPageInfoDTO> vidInfoList = soaResponse.getResponseVo().getVidInfos();
        List<Long> parentVidList = vidInfoList.stream()
                .map(VidPageInfoDTO::getParentVid).filter(o -> !DEFAULT_VID.equals(o))
                .distinct().collect(Collectors.toList());
        Long selectVid = reqVo.getSelectVid();
        if (Objects.nonNull(selectVid) && !parentVidList.contains(selectVid)) {
            parentVidList.add(selectVid);
        }
        Map<Long, com.weimob.mp.merchantstructure.common.domain.common.VidInfoDto> vidNameMap = structureFacade.getVidMap(parentVidList);
        List<AttributionNodeDto> nodeDtos = new ArrayList<>();
        final Long paramParentVid = parentVid;
        EnhancedBeanCopyUtils.copy(vidInfoList, nodeDtos, AttributionNodeDto.class, (o1, o2) -> {
            o2.setVidType(o1.getVidTypes().get(0));
            o2.setParentVidName(Objects.nonNull(vidNameMap.get(o1.getParentVid())) ? vidNameMap.get(o1.getParentVid()).getBaseInfo().getVidName() : null);
            o2.setVidName(o1.getBaseInfo().getVidName());
            o2.setHasChildren(o1.isHasChildren());
            if (o1.getVid().equals(paramParentVid)) {
                o2.setIsParamParentVid(Boolean.TRUE);
            }
        });
        AttributionSelectResVo<AttributionNodeDto> resVo = new AttributionSelectResVo<>();
        resVo.setPageNumber(soaResponse.getResponseVo().getPage());
        resVo.setPageSize(soaResponse.getResponseVo().getSize());
        resVo.setMore(soaResponse.getResponseVo().getMore());
        resVo.setVidInfoList(nodeDtos);

        com.weimob.mp.merchantstructure.common.domain.common.VidInfoDto vidInfoDto = vidNameMap.get(selectVid);
        resVo.setSelectVidName(Objects.nonNull(vidInfoDto) ? vidInfoDto.getBaseInfo().getVidName() : null);
        if (Objects.nonNull(vidInfoDto) && !DEFAULT_VID.equals(vidInfoDto.getParentVid())) {
            resVo.setSelectVidParentName(structureFacade.getVidName(vidInfoDto.getParentVid()));
        }
        return resVo;
    }

    @Override
    public NodeSelectionResVo<AttributionNodeDto> nodeSelection(NodeSelectionReqVo reqVo) {
        QueryMultipleVidPageReqDTO request = new QueryMultipleVidPageReqDTO();
        QueryMultipleVidDetailReqDTO detail = new QueryMultipleVidDetailReqDTO();
        request.setPage(reqVo.getPageNum());
        request.setSize(reqVo.getPageSize());
        request.setBosId(reqVo.getBosId());
        List<Integer> vidTypes = reqVo.getVidTypeList().stream().filter(Objects::nonNull).collect(toList());
        detail.setVidTypes(vidTypes);
        detail.setVidStatus(reqVo.getVidStatus());
        detail.setVidList(reqVo.getVidList());
        if (DEFAULT_VID.equals(reqVo.getParentVid())) {
            reqVo.setParentVid(null);
        }
        if (StringUtils.isNotBlank(reqVo.getVidName())) {
            detail.setVidName(reqVo.getVidName());
            detail.setDepthLayer(Boolean.TRUE);
        }
        if (CollectionUtils.isNotEmpty(vidTypes) && vidTypes.size() == 1) {
            detail.setDepthLayer(Boolean.TRUE);
        }
        if (CollectionUtils.isNotEmpty(reqVo.getProductIdList())) {
            detail.setProductIdList(reqVo.getProductIdList());
        }
        if (CollectionUtils.isNotEmpty(reqVo.getProductInstanceIdList())) {
            detail.setProductInstanceIdList(reqVo.getProductInstanceIdList());
        }
        //排序设置
        VidPageOrderReqDTO sort = new VidPageOrderReqDTO();
        sort.setField(VidPageOrderFiledEnum.CREATE_TIME.getField());
        sort.setDesc(false);
        List<VidPageOrderReqDTO> sorts = Arrays.asList(sort);
        request.setSorts(sorts);
        List<String> resultData = new ArrayList<>();
        Long parentVid = reqVo.getParentVid();
        Integer parentVidType = DEFAULT_VID_TYPE;
        if (Objects.isNull(parentVid)) {
            RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
            if (Objects.nonNull(rootNode)) {
                parentVid = structureFacade.getRootNodeVid(reqVo.getBosId());
                parentVidType = rootNode.getVidType();
            }
        } else {
            parentVidType = structureFacade.getVidInfo(parentVid).getVidTypes().get(0);
        }
        detail.setParentVid(parentVid);
        if (reqVo.getIsContainParent()) {
            resultData.add(NodeSearchParentNodeEnum.PARENT_NODE.getCode());
        }
        if (CollectionUtils.isEmpty(vidTypes)) {
            resultData.add(NodeSearchParentNodeEnum.HAS_CHILD.getCode());
        }
        request.setResultData(resultData);
        //业务组类型查询参数处理
        if (reqVo.getGroupType() != null || reqVo.getGroupId() != null
                || reqVo.getUnrelatedGroupType() != null) {
            SearchBusinessGroupReqDTO groupReqDTO = new SearchBusinessGroupReqDTO();
            if (reqVo.getUnrelatedGroupType() != null && reqVo.getUnrelatedGroupType()) {
                GEPreconditions.checkState(Objects.nonNull(reqVo.getGroupType()), MpBizCode.GROUP_TYPE_ERROR);
                groupReqDTO.setFilter(Boolean.TRUE);
            } else {
                groupReqDTO.setFilter(Boolean.FALSE);
            }
            groupReqDTO.setGroupType(reqVo.getGroupType());
            if (reqVo.getGroupId() != null) {
                groupReqDTO.setGroupIdList(Collections.singletonList(reqVo.getGroupId()));
            }
            request.setBusinessGroup(groupReqDTO);
        }
        List<VidPageExtMatchReqDTO> extMatchList = new ArrayList<>();
        if (Objects.nonNull(reqVo.getWxOnlineStatus()) && (reqVo.getVidTypeList().contains(VidTypeEnum.BRAND
                .getType()) || reqVo.getVidTypeList().contains(VidTypeEnum.STORE.getType()) || reqVo.getVidTypeList().contains(VidTypeEnum.MALL.getType()))) {
            VidPageExtMatchReqDTO wxOnlineStatusReq = new VidPageExtMatchReqDTO();
            wxOnlineStatusReq.setProductId(Constants.DEFAULT_PRODUCT_ID);
            wxOnlineStatusReq.setSettingKey(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey());
            wxOnlineStatusReq.setSettingValues(Arrays.asList(reqVo.getWxOnlineStatus().toString()));
            extMatchList.add(wxOnlineStatusReq);
        }
        detail.setExtMatchList(extMatchList);
        List<QueryMultipleVidDetailReqDTO> details = new ArrayList<>();
        details.add(detail);
        request.setDetails(details);
        List<Long> vidList = null;
        if (reqVo.getQueryAllParentVid() && Objects.nonNull(reqVo.getParentVid()) && Constants.DEFAULT_FIRST_PAGE_NUM.equals(reqVo.getPageNum())) {
            QueryVidInfoResponseDto vidInfo = structureFacade.getVidInfo(reqVo.getParentVid());
            vidList = vidInfo.getParentPathTree();
            if (CollectionUtils.isNotEmpty(vidList)) {
                QueryMultipleVidDetailReqDTO parentDetail = new QueryMultipleVidDetailReqDTO();
                parentDetail.setVidList(vidList);
                parentDetail.setVidTypes(vidTypes);
                request.getDetails().add(parentDetail);
            }
        }
        //查询指定扩展字段
        VidPageExtInfoReqDTO queryVidExtInfoDto = new VidPageExtInfoReqDTO();
        queryVidExtInfoDto.setProductId(Constants.DEFAULT_PRODUCT_ID);
        Long productInstanceId = productFacade.getDefaultProductInstanceId(request.getBosId());
        queryVidExtInfoDto.setProductInstanceId(productInstanceId);
        queryVidExtInfoDto.setExtFields(Arrays.asList(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey()));
        request.setExtInfos(Arrays.asList(queryVidExtInfoDto));
        //区域特殊处理
        List<VidPageInfoDTO> vidInfoList = new ArrayList<>();
        Boolean more;

        //跨级查询一级区域  如果按名字搜索则不走此逻辑
        if (vidTypes.size() == 1 && vidTypes.contains(VidTypeEnum.DISTRICT.getType()) && parentVidType.equals
                (VidTypeEnum.GROUP.getType()) && StringUtils.isBlank(reqVo.getVidName())) {
            List<VidPageInfoDTO> vidInfos = new ArrayList<>();
            request.setSize(Constants.DEFAULT_MAX_PAGE_SIZE);
            Integer pageNum = Constants.DEFAULT_FIRST_PAGE_NUM;
            more = Boolean.TRUE;
            Integer maxTime = 0;
            do {
                request.setPage(pageNum);
                SoaResponse<VidPageRespDTO, Void> soaResponse = nodeSearchExportService.queryMultipleVidPage2(request);
                GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
                vidInfos.addAll(soaResponse.getResponseVo().getVidInfos());
                more = soaResponse.getResponseVo().getMore();
                maxTime++;
                pageNum++;
            } while (more && maxTime < Constants.SCROLL_TIMES);
            List<Long> parentVids = vidInfos.stream().map(VidPageInfoDTO::getVid).collect(toList());
            //过滤出父节点不在此列表中的则为一级区域
            List<VidPageInfoDTO> resultList = vidInfos.stream().filter(vid -> !parentVids.contains(vid.getParentVid())).collect(toList());
            Integer start = (reqVo.getPageNum() - 1) * reqVo.getPageSize();
            Integer end = reqVo.getPageNum() * reqVo.getPageSize();
            if (resultList.size() > end) {
                more = Boolean.TRUE;
            } else {
                more = Boolean.FALSE;
                end = resultList.size();
            }
            List<VidPageInfoDTO> subList = resultList.subList(start, end);
            vidInfoList.addAll(subList);
        } else {
            //直接挂载或二三级区域查找不做深度搜索 如果vidName不为空则深度搜索
            if (vidTypes.size() == 1 && vidTypes.contains(VidTypeEnum.DISTRICT.getType()) && StringUtils.isBlank(reqVo.getVidName())) {
                detail.setDepthLayer(Boolean.FALSE);
            }
            SoaResponse<VidPageRespDTO, Void> soaResponse = nodeSearchExportService.queryMultipleVidPage2(request);
            GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
            vidInfoList = soaResponse.getResponseVo().getVidInfos();
            more = soaResponse.getResponseVo().getMore();
        }
        //区域单独查找是否包含子节点
        if (vidTypes.size() == 1 && vidTypes.contains(VidTypeEnum.DISTRICT.getType()) && StringUtils.isBlank(reqVo.getVidName())) {
            List<Long> vids = vidInfoList.stream().map(VidPageInfoDTO::getVid).distinct().collect(toList());
            if (CollectionUtils.isNotEmpty(vids)) {
                QueryChildCountReqDto reqDTO = new QueryChildCountReqDto();
                reqDTO.setBosId(reqVo.getBosId());
                reqDTO.setParentVidList(vids);
                reqDTO.setVidType(VidTypeEnum.DISTRICT.getType());
                reqDTO.setAllLayout(Boolean.FALSE);
                reqDTO.setBusinessGroup(request.getBusinessGroup());
                SoaResponse<ParentToChildCountRespDto, Void> childSoaResponse = merchantStructureExportService.findChildVidCountByParents(reqDTO);
                GEPreconditions.checkState(SoaRespUtils.isSuccess(childSoaResponse), MpBizCode.build(childSoaResponse));
                Map<Long, Integer> childMap = childSoaResponse.getResponseVo().getChildCountMap();
                vidInfoList.forEach(vid -> {
                    if (Objects.nonNull(childMap.get(vid.getVid())) && childMap.get(vid.getVid()) > 0) {
                        vid.setHasChildren(Boolean.TRUE);
                    }
                });
            }
        }

        List<Long> queryVidList = vidInfoList.stream()
                .map(VidPageInfoDTO::getParentVid).filter(o -> !DEFAULT_VID.equals(o))
                .distinct().collect(Collectors.toList());
        List<Long> selectVids = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(reqVo.getSelectVids())) {
            selectVids = reqVo.getSelectVids().stream().filter(Objects::nonNull).collect(toList());
            if (CollectionUtils.isNotEmpty(selectVids)) {
                queryVidList.addAll(selectVids);
            }
        }

        if (reqVo.getIsQueryPathName()) {
            List<Long> pathList = vidInfoList.stream()
                    .map(VidPageInfoDTO::getParentPathTree)
                    .flatMap(Collection::stream).collect(toList());
            queryVidList.addAll(pathList);
        }

        Map<Long, com.weimob.mp.merchantstructure.common.domain.common.VidInfoDto> vidNameMap = structureFacade.getVidMap(queryVidList);
        List<AttributionNodeDto> nodeDtos = new ArrayList<>();
        final Long paramParentVid = parentVid;
        EnhancedBeanCopyUtils.copy(vidInfoList, nodeDtos, AttributionNodeDto.class, (o1, o2) -> {
            o2.setVidType(o1.getVidTypes().get(0));
            o2.setParentVidName(Objects.nonNull(vidNameMap.get(o1.getParentVid())) ? vidNameMap.get(o1.getParentVid()).getBaseInfo().getVidName() : null);
            o2.setVidName(o1.getBaseInfo().getVidName());
            o2.setHasChildren(o1.isHasChildren());
            o2.setVidStatus(o1.getBaseInfo().getVidStatus());
            if (o1.getVid().equals(paramParentVid)) {
                o2.setIsParamParentVid(Boolean.TRUE);
            }
            if (CollectionUtils.isNotEmpty(o1.getExtInfos())) {
                String wxOnlineStatus = o1.getExtInfos().get(0).getExtInfoMap().get(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey());
                if (StringUtils.isNotBlank(wxOnlineStatus)) {
                    o2.setWxOnlineStatus(Integer.parseInt(wxOnlineStatus));
                }
            }
            if (reqVo.getIsQueryPathName()) {
                StringBuilder pathName = new StringBuilder();
                o1.getParentPathTree().forEach(vid -> {
                    if (Objects.nonNull(vidNameMap.get(vid))) {
                        pathName.append(vidNameMap.get(vid).getBaseInfo().getVidName()).append("/");
                    }
                });
                pathName.append(o1.getBaseInfo().getVidName());
                o2.setVidPathName(pathName.toString());
            }
        });
        NodeSelectionResVo<AttributionNodeDto> resVo = new NodeSelectionResVo<>();
        if (CollectionUtils.isNotEmpty(selectVids)) {
            List<NodeSelectionResVo<AttributionNodeDto>.SelectVidInfo> vidInfos = new ArrayList<>();
            List<Long> selectVidPathList = new ArrayList<>();
            List<String> selectVidNames = new ArrayList<>();
            selectVids.forEach(vid -> {
                if (Objects.nonNull(vidNameMap.get(vid))) {
                    selectVidPathList.addAll(vidNameMap.get(vid).getParentPathTree());
                }
            });
            selectVidPathList.addAll(selectVids);
            List<QueryVidExtInfoDto> vidExtInfos = new ArrayList<>();
            QueryVidExtInfoDto extInfoDto = new QueryVidExtInfoDto();
            extInfoDto.setProductId(Constants.DEFAULT_PRODUCT_ID);
            extInfoDto.setProductInstanceId(productInstanceId);
            extInfoDto.setExtFields(Arrays.asList(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey()));
            vidExtInfos.add(extInfoDto);
            Map<Long, com.weimob.mp.merchantstructure.common.domain.common.VidInfoDto> selectVidInfoMap = structureFacade
                    .selectVidInfoMap(selectVidPathList, reqVo.getBosId(), vidExtInfos);
            for (Long selectVid : selectVids) {
                com.weimob.mp.merchantstructure.common.domain.common.VidInfoDto vidInfoDto = selectVidInfoMap.get(selectVid);
                if (Objects.nonNull(vidInfoDto)) {
                    NodeSelectionResVo<AttributionNodeDto>.SelectVidInfo vidInfo = resVo.new SelectVidInfo();
                    vidInfo.setSelectVid(selectVid);
                    vidInfo.setSelectVidName(vidInfoDto.getBaseInfo().getVidName());
                    vidInfo.setSelectVidPath(vidNameMap.get(selectVid).getPath());
                    vidInfo.setVidStatus(vidNameMap.get(selectVid).getBaseInfo().getVidStatus());
                    if (CollectionUtils.isNotEmpty(selectVidInfoMap.get(selectVid).getExtInfos())) {
                        String wxOnlineStatus = selectVidInfoMap.get(selectVid).getExtInfos().get(0).getExtInfoMap().get(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey());
                        if (StringUtils.isNotBlank(wxOnlineStatus)) {
                            vidInfo.setWxOnlineStatus(Integer.parseInt(wxOnlineStatus));
                        }
                    }
                    StringBuilder pathName = new StringBuilder();
                    vidInfoDto.getParentPathTree().forEach(vid -> {
                        if (Objects.nonNull(vidNameMap.get(vid))) {
                            pathName.append(vidNameMap.get(vid).getBaseInfo().getVidName()).append("/");
                        }
                    });
                    pathName.append(vidInfoDto.getBaseInfo().getVidName());
                    vidInfo.setSelectVidPathName(pathName.toString());
                    vidInfos.add(vidInfo);
                    selectVidNames.add(vidInfoDto.getBaseInfo().getVidName());
                }
            }
            resVo.setSelectVidInfoList(vidInfos);
            resVo.setSelectVidNameList(selectVidNames);
        }
        resVo.setPageNumber(reqVo.getPageNum());
        resVo.setPageSize(reqVo.getPageSize());
        resVo.setMore(more);
        resVo.setVidInfoList(nodeDtos);
        return resVo;
    }

    @Override
    public HttpResponse<SelectStoreVidTreeResVo> selectStoreVidTree(SelectStoreVidTreeReqVo reqVo) {
        QueryVidPageReqDTO request = new QueryVidPageReqDTO();
        if (reqVo.getPageSize() > Constants.MAX_PAGE_SIZE) {
            reqVo.setPageSize(Constants.MAX_PAGE_SIZE);
        }
        request.setPage(reqVo.getPageNum());
        request.setSize(reqVo.getPageSize());
        request.setBosId(reqVo.getBosId());
        request.setVidTypes(reqVo.getVidType());
        request.setTagIds(reqVo.getTagIdList());
        request.setProductInstanceIdList(reqVo.getProductInstanceIds());
        request.setVidName(reqVo.getVidName());
        request.setVidCode(reqVo.getVidCode());
        request.setCodeFuzzyMatch(Boolean.TRUE);
        request.setVidStatus(reqVo.getVidStatus());
        request.setVidLabels(reqVo.getVidLabels());
        request.setVidList(reqVo.getVidList());
        //业务组类型查询参数处理
        if (reqVo.getGroupType() != null || reqVo.getGroupId() != null
                || reqVo.getUnrelatedGroupType() != null) {
            SearchBusinessGroupReqDTO groupReqDTO = new SearchBusinessGroupReqDTO();
            if (reqVo.getUnrelatedGroupType() != null && reqVo.getUnrelatedGroupType()) {
                GEPreconditions.checkState(Objects.nonNull(reqVo.getGroupType()), MpBizCode.GROUP_TYPE_ERROR);
                groupReqDTO.setFilter(Boolean.TRUE);
            } else {
                groupReqDTO.setFilter(Boolean.FALSE);
            }
            groupReqDTO.setGroupType(reqVo.getGroupType());
            if (reqVo.getGroupId() != null) {
                groupReqDTO.setGroupIdList(Collections.singletonList(reqVo.getGroupId()));
            }
            request.setBusinessGroup(groupReqDTO);
        }
        List<VidPageExtMatchReqDTO> extMatchList = new ArrayList<>();
        structureFacade.setQueryAddress(extMatchList, BeanCopyUtils.copy(reqVo, AddressDTO.class));
        if (Objects.nonNull(reqVo.getWxOnlineStatus())) {
            VidPageExtMatchReqDTO wxOnlineStatusReq = new VidPageExtMatchReqDTO();
            wxOnlineStatusReq.setProductId(Constants.DEFAULT_PRODUCT_ID);
            wxOnlineStatusReq.setSettingKey(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey());
            wxOnlineStatusReq.setSettingValues(Arrays.asList(reqVo.getWxOnlineStatus().toString()));
            extMatchList.add(wxOnlineStatusReq);
        }
        if (Objects.nonNull(reqVo.getFloorNumber())) {
            VidPageExtMatchReqDTO floorNumberReq = new VidPageExtMatchReqDTO();
            floorNumberReq.setProductId(Constants.DEFAULT_PRODUCT_ID);
            floorNumberReq.setSettingKey(ExtFieldKeyEnum.FLOOR_NAME.getKey());
            floorNumberReq.setSettingValues(Arrays.asList(reqVo.getFloorNumber().toString()));
            extMatchList.add(floorNumberReq);
        }
        if (Objects.nonNull(reqVo.getRetailFormatId())) {
            VidPageExtMatchReqDTO formatIdReq = new VidPageExtMatchReqDTO();
            formatIdReq.setProductId(Constants.BUSINESS_DISTRICT_PRODUCT_ID);
            formatIdReq.setSettingKey(ExtFieldKeyEnum.RETAIL_FORMAT_ID.getKey());
            formatIdReq.setSettingValues(Arrays.asList(reqVo.getRetailFormatId().toString()));
            extMatchList.add(formatIdReq);
        }

        //根据父id查询
        if (Objects.nonNull(reqVo.getDistrictId()) || CollectionUtils.isNotEmpty(reqVo.getParentVidList())) {
            request.setParentVid(reqVo.getDistrictId());
            request.setParentVidList(reqVo.getParentVidList());
            request.setDepthLayer(Boolean.TRUE);
            if (reqVo.getIsContainParent()) {
                request.setResultData(Arrays.asList(NodeSearchParentNodeEnum.PARENT_NODE.getCode()));
            }
        }
        request.setExtMatchList(extMatchList);
        //按照sort排序
        VidPageOrderReqDTO sort = new VidPageOrderReqDTO();
        sort.setField(VidPageOrderFiledEnum.SORT.getField());
        sort.setDesc(true);
        List<VidPageOrderReqDTO> sorts = Arrays.asList(sort);
        request.setSorts(sorts);
        PageInfo<NodeTreeInfoDto> nodeTreeInfoDtoPageInfo = structureFacade.selectNodeTree(request);
        SelectStoreVidTreeResVo resVo = new SelectStoreVidTreeResVo();
        resVo.setVidInfoList(nodeTreeInfoDtoPageInfo.getData());
        nodeTreeInfoDtoPageInfo.setData(null);
        resVo.setPageInfo(nodeTreeInfoDtoPageInfo);
        return SoaTransformUtil.success(resVo);
    }

    @Override
    public HttpResponse<SelectDistrictListResVo> selectDistrictList(SelectDistrictListReqVo reqVo) {
        QueryVidPageReqDTO request = new QueryVidPageReqDTO();
        if (reqVo.getPageSize() > Constants.MAX_PAGE_SIZE) {
            reqVo.setPageSize(Constants.MAX_PAGE_SIZE);
        }
        request.setPage(reqVo.getPageNum());
        request.setSize(reqVo.getPageSize());
        request.setBosId(reqVo.getBosId());
        request.setVidTypes(Collections.singletonList(VidTypeEnum.DISTRICT.getType()));
        request.setVidName(reqVo.getVidName());
        request.setVidCode(reqVo.getVidCode());
        request.setCodeFuzzyMatch(Boolean.TRUE);
        if (reqVo.getParentVid() != null) {
            request.setParentVidList(Collections.singletonList(reqVo.getParentVid()));
            request.setDepthLayer(Boolean.TRUE);
        }
        //业务组类型查询参数处理
        if (reqVo.getGroupType() != null || reqVo.getGroupId() != null
                || reqVo.getUnrelatedGroupType() != null) {
            SearchBusinessGroupReqDTO groupReqDTO = new SearchBusinessGroupReqDTO();
            if (reqVo.getUnrelatedGroupType() != null && reqVo.getUnrelatedGroupType()) {
                GEPreconditions.checkState(Objects.nonNull(reqVo.getGroupType()), MpBizCode.GROUP_TYPE_ERROR);
                groupReqDTO.setFilter(Boolean.TRUE);
            } else {
                groupReqDTO.setFilter(Boolean.FALSE);
            }
            groupReqDTO.setGroupType(reqVo.getGroupType());
            if (reqVo.getGroupId() != null) {
                groupReqDTO.setGroupIdList(Collections.singletonList(reqVo.getGroupId()));
            }
            request.setBusinessGroup(groupReqDTO);
        }
        PageInfo<DistrictNodeInfoDto> districtNodeInfoDtoPageInfo = structureFacade.selectDistrictNodeInfoList(request);
        GEPreconditions.checkState(Objects.nonNull(districtNodeInfoDtoPageInfo), MpBizCode.NOT_FOUND_NODE_LIST);
        SelectDistrictListResVo selectDistrictListResVo = new SelectDistrictListResVo();
        PageInfo pageInfo = new PageInfo<>();
        pageInfo.setPageNumber(districtNodeInfoDtoPageInfo.getPageNumber());
        pageInfo.setPageSize(districtNodeInfoDtoPageInfo.getPageSize());
        pageInfo.setTotalCount(districtNodeInfoDtoPageInfo.getTotalCount());
        selectDistrictListResVo.setVidInfoList(districtNodeInfoDtoPageInfo.getData());
        selectDistrictListResVo.setPageInfo(pageInfo);
        return SoaTransformUtil.success(selectDistrictListResVo);

    }

    @Override
    public HttpResponse<ActiveProductNodeTreeResVo> activeProductNodeTree(ActiveProductNodeTreeReqVo reqVo) {
        QueryVidPageReqDTO request = new QueryVidPageReqDTO();
        request.setSize(Constants.DEFAULT_MAX_PAGE_SIZE);
        request.setBosId(reqVo.getBosId());
        request.setVidTypes(reqVo.getVidTypeList());
        request.setVidName(reqVo.getVidName());
        List<NodeTreeInfoDto> nodeTreeInfoDtos = new ArrayList<>();
        Integer currentPage = Constants.DEFAULT_FIRST_PAGE_NUM;

        int totalPage = PageUtil.totalPage(whileLimitCounterConfig.getBosIdVidMaxSize(), Constants.DEFAULT_MAX_PAGE_SIZE);
        int counter = 0;
        List<NodeTreeInfoDto> nodeInfoDtos;
        do {
            counter++;
            request.setPage(currentPage);
            nodeInfoDtos = structureFacade.selectNodeTree(request).getData();
            if (CollectionUtils.isNotEmpty(nodeInfoDtos)) {
                nodeTreeInfoDtos.addAll(nodeInfoDtos);
            }
            currentPage += 1;
        } while (CollectionUtils.isNotEmpty(nodeInfoDtos) && counter <= totalPage);
        if (counter > totalPage && CollectionUtils.isNotEmpty(nodeInfoDtos)) {
            //触发日志告警
            log.error("The number of pages reached the upper limit but has more.",
                    new PageReachedUpperLimitException("while.limit.counter.bosId_vid_max_size"));
        }

        Map<Integer, Long> vidTypeNum = productFacade.getVidTypeNumByProductInstanceId(reqVo.getBosId(), reqVo.getProductInstanceId());
        //已开通此产品节点
        List<Long> vidList = productFacade.getAllActiveNodeByInstanceId(reqVo.getBosId(), reqVo.getProductInstanceId()).stream().map
                (ProductActivationFlowDTO::getVid).collect(toList());
        List<NodeTreeDto> nodeTreeList = new ArrayList<>();
        recursiveTree(nodeTreeInfoDtos, nodeTreeList, vidList);
        ActiveProductNodeTreeResVo resVo = new ActiveProductNodeTreeResVo();
        resVo.setNodeTreeList(nodeTreeList);
        resVo.setVidTypeNum(vidTypeNum);
        return SoaTransformUtil.success(resVo);
    }

    @Override
    public HttpResponse<ActiveProductVidListResVo> activeProductVidList(ActiveProductNodeTreeReqVo reqVo) {
        List<Long> vidList = productFacade.getAllActiveNodeByInstanceId(reqVo.getBosId(), reqVo.getProductInstanceId()).stream().map
                (ProductActivationFlowDTO::getVid).collect(toList());
        List<ProductInstanceFlowDTO> bosProductInstanceId = productFacade.getBosProductInstanceId(reqVo.getBosId());
        if (Objects.nonNull(reqVo.getProductInstanceId())) {
            bosProductInstanceId = bosProductInstanceId.stream().filter(dto -> reqVo.getProductInstanceId().equals(dto.getProductInstanceId())).collect(toList());
        }
        List<Long> productInstanceIdList = bosProductInstanceId.stream()
                .map(ProductInstanceFlowDTO::getProductInstanceId).collect(Collectors.toList());
        List<AttachedFunctionDTO> attachedFunctionDTOS = productFacade.getVidTypeNumByProductInstanceId
                (productInstanceIdList);
        Map<Integer, Long> vidTypeNum = productFacade.getAttachedMap(attachedFunctionDTOS, bosProductInstanceId);
        Map<Integer, Long> resultMap = new HashMap<>();
        generateVidTypeNum(attachedFunctionDTOS, vidTypeNum, resultMap, reqVo.getProductInstanceId(),reqVo.getBosId(),false);
        ActiveProductVidListResVo resVo = new ActiveProductVidListResVo(vidList,resultMap);
        return SoaTransformUtil.success(resVo);
    }


    @Override
    public HttpResponse<ProductVidTypeNumResVo> productVidTypeNum(ProductVidTypeNumReqVo reqVo) {
        List<ProductInstanceFlowDTO> bosProductInstanceId = productFacade.getBosProductInstanceId(reqVo.getBosId());
        if (Objects.nonNull(reqVo.getProductInstanceId())) {
            bosProductInstanceId = bosProductInstanceId.stream().filter(dto -> reqVo.getProductInstanceId().equals(dto.getProductInstanceId())).collect(toList());
        }
        List<Long> productInstanceIdList = bosProductInstanceId.stream()
                .map(ProductInstanceFlowDTO::getProductInstanceId).collect(Collectors.toList());
        List<AttachedFunctionDTO> attachedFunctionDTOS = productFacade.getVidTypeNumByProductInstanceId
                (productInstanceIdList);
        Map<Integer, Long> vidTypeNum = productFacade.getAttachedMap(attachedFunctionDTOS, bosProductInstanceId);
        Map<Integer, Long> resultMap = new HashMap<>();
        generateVidTypeNum(attachedFunctionDTOS, vidTypeNum, resultMap, reqVo.getProductInstanceId(),reqVo.getBosId(),true);
        ProductVidTypeNumResVo resVo = new ProductVidTypeNumResVo(resultMap);
        return SoaTransformUtil.success(resVo);
    }

    private void generateVidTypeNum(List<AttachedFunctionDTO> attachedFunctionDTOS, Map<Integer, Long> vidTypeNum,
            Map<Integer, Long> resultMap, Long productInstanceId,Long bosId,Boolean isFilter) {
        Map<Integer, Integer> withoutTypeMap = structureFacade
                .checkVidTypeListWithoutProductInstanceByBosId(bosId,productInstanceId);

        attachedFunctionDTOS.forEach(attached -> {
            if (productInstanceId.equals(attached.getInstanceId().getId())) {
                Map<String, Object> functionMap = attached.getFunctions();
                if (MapUtils.isNotEmpty(functionMap) && Objects.nonNull(functionMap.get(PublicAuxiliaryFunctionEnum
                        .ADAPTER_VID_TYPE_OPTION.getKey()))) {
                    List<Integer> vidTypeList = JSONArray.parseArray(functionMap.get(PublicAuxiliaryFunctionEnum.ADAPTER_VID_TYPE_OPTION.getKey()).toString(), Integer.class);
                    vidTypeList.forEach(type -> {
                        Long num = vidTypeNum.get(type);
                        if (Objects.nonNull(num) && num > 0 && (!isFilter || withoutTypeMap.containsKey(type))) {
                            resultMap.put(type,num);
                        }
                    });
                }
            }
        });
    }

    private void recursiveTree(List<NodeTreeInfoDto> nodeTreeInfoDtos, List<NodeTreeDto> nodeTreeList, List<Long>
            vidList) {
        if (CollectionUtils.isEmpty(nodeTreeInfoDtos)) {
            return;
        }
        Map<Long, List<NodeTreeInfoDto>> nodeMap = new HashMap<>();
        for (NodeTreeInfoDto node : nodeTreeInfoDtos) {
            if (Objects.nonNull(node.getParentVid())) {
                List<NodeTreeInfoDto> nodeList = nodeMap.get(node.getParentVid());
                if (CollectionUtils.isEmpty(nodeList)) {
                    List<NodeTreeInfoDto> treeList = Lists.newArrayList(node);
                    nodeMap.put(node.getParentVid(), treeList);
                } else {
                    nodeList.add(node);
                }
            }
        }
        //获取根节点
        List<NodeTreeInfoDto> first = nodeTreeInfoDtos.stream().filter(o -> Objects.isNull(o.getParentVid()) ||
                Constants.DEFAULT_VID.equals(o.getParentVid()))
                .collect
                        (toList());
        if (CollectionUtils.isEmpty(first)) {
            return;
        }
        recursiveTree(nodeMap, first, nodeTreeList, vidList);
    }

    private void recursiveTree(Map<Long, List<NodeTreeInfoDto>> nodeMap, List<NodeTreeInfoDto> nodeList,
                               List<NodeTreeDto> nodeTreeList, List<Long> vidList) {
        for (NodeTreeInfoDto node : nodeList) {
            NodeTreeDto nodeTreeDto = BeanCopyUtils.copy(node, NodeTreeDto.class);
            if (vidList.contains(nodeTreeDto.getVid())) {
                nodeTreeDto.setIsOpened(Boolean.TRUE);
            }
            List<NodeTreeInfoDto> childList = nodeMap.get(node.getVid());
            if (CollectionUtils.isEmpty(childList)) {
                nodeTreeDto.setChildNodeList(null);
            } else {
                List<NodeTreeDto> nodeTreeDtos = new ArrayList<>();
                recursiveTree(nodeMap, childList, nodeTreeDtos, vidList);
                nodeTreeDto.setChildNodeList(nodeTreeDtos);
            }
            nodeTreeList.add(nodeTreeDto);
        }
    }

    @Override
    public HttpResponse<List<ProductSelectResVo>> productSelect(ProductSelectReqVo reqVo) {
        Long vid = reqVo.getVid();
        List<ProductSelectResVo> resVos = new ArrayList<>();
        List<ProductInstanceFlowDTO> productInstanceFlowDTOS;
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
        if (vid.equals(Constants.DEFAULT_VID) || (Objects.nonNull(rootNode) && rootNode.getVid().equals(vid))) {
            productInstanceFlowDTOS = productFacade.getBosProductInstanceId(reqVo.getBosId());
        } else {
            productInstanceFlowDTOS = productFacade.getVidProductInstanceId(vid);
        }
        if (CollectionUtils.isNotEmpty(productInstanceFlowDTOS)) {
            Map<Long, List<ProductInstanceFlowDTO>> collectMap = productInstanceFlowDTOS.stream().collect(Collectors.groupingBy(ProductInstanceFlowDTO::getProductId));
            for (Long productId : collectMap.keySet()) {
                ProductSelectResVo resVo = new ProductSelectResVo();
                List<ProductInstanceFlowDTO> productInstances = collectMap.get(productId);
                BeanCopyUtils.copy(productInstances.get(0), resVo);
                List<Long> productInstanceIdList = new ArrayList<>();
                for (ProductInstanceFlowDTO productInstance : productInstances) {
                    productInstanceIdList.add(productInstance.getProductInstanceId());
                }
                resVo.setProductInstanceIdList(productInstanceIdList);
                resVos.add(resVo);
            }
        }
        return SoaTransformUtil.success(resVos);
    }

    @Override
    public List<productListVo> productListForVid(ProductListForVidReqVo reqVo) {
        List<ProductInstanceFlowDTO> productInstanceFlowDTOS;
        Long vid = reqVo.getVid();
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());

        List<Integer> filterVidType = new ArrayList<>();
        if (Objects.nonNull(reqVo.getSelectVidType())) {
            filterVidType.add(reqVo.getSelectVidType());
            if (!DEFAULT_VID.equals(reqVo.getVid()) &&
                    (Objects.nonNull(rootNode) && rootNode.getVidType().equals(reqVo.getSelectVidType()))) {
                filterVidType.add(DEFAULT_VID_TYPE);
            }
        }
        if (vid.equals(Constants.DEFAULT_VID) || (Objects.nonNull(rootNode) && rootNode.getVid().equals(vid))) {
            productInstanceFlowDTOS = productFacade.getBosProductInstanceId(reqVo.getBosId());
        } else {
            productInstanceFlowDTOS = productFacade.getVidProductInstanceId(reqVo.getVid());
        }
        List<Long> productInstanceIds = productInstanceFlowDTOS.stream().map(ProductInstanceFlowDTO::
                getProductInstanceId).distinct().collect(toList());
        //根据菜单及组织类型过滤产品
        List<MenuDTO> menuList = productFacade
                .getMenuListByProductInstance(productInstanceIds, SystemChannelEnum.PC, null);
        Map<ProductInstanceId, List<MenuItem>> menuMap = menuList.stream()
                .filter(menu -> CollectionUtils.isNotEmpty(menu.getMenus())).collect(Collectors.toMap
                        (MenuDTO::getInstanceId, MenuDTO::getMenus));
        List<Long> filterInstance = productInstanceIds.stream().filter(id -> {
            ProductInstanceId instanceId = new ProductInstanceId(id);
            if (!menuMap.keySet().contains(instanceId)) {
                return false;
            }
            if (CollectionUtils.isNotEmpty(filterVidType)) {
                List<MenuItem> menuItems = menuMap.get(instanceId).stream()
                        .filter(item -> CollectionUtils.isNotEmpty(item.getVidTypes()) &&
                                !Collections.disjoint(item.getVidTypes(), filterVidType)).collect(toList());
                if (CollectionUtils.isEmpty(menuItems)) {
                    return false;
                }
            }
            return true;
        }).collect(toList());
        //附属功能
        List<AttachedFunctionDTO> AttachedFunctionList = productFacade
                .getVidTypeNumByProductInstanceId(filterInstance);
        Map<Long, Map<String, Object>> instanceIdMapMap = AttachedFunctionList.stream()
                .collect(Collectors.toMap(dto -> dto.getInstanceId().getId(),
                        AttachedFunctionDTO::getFunctions));
        List<productListVo> resList = new ArrayList<>();
        productInstanceFlowDTOS.forEach(instance -> {
            if (!filterInstance.contains(instance.getProductInstanceId())) {
                return;
            }
            productListVo productVo = BeanCopyUtils.copy(instance, productListVo.class);
            Map<String, Object> functionMap = instanceIdMapMap.get(instance.getProductInstanceId());
            Set<Integer> vidTypeList = new HashSet<>();
            if (MapUtils.isNotEmpty(functionMap)) {
                Object bosActivateSwitch = functionMap.get(PublicAuxiliaryFunctionEnum.BOS_ACTIVATE_SWITCH.getKey());
                if (PublicIsOrNotEnum.TRUE.getIntCode().toString().equals(bosActivateSwitch) || PublicIsOrNotEnum.TRUE
                        .getBooleanCode().equals(bosActivateSwitch)) {
                    vidTypeList.add(rootNode.getVidType());
                }
                if (Objects.nonNull(functionMap.get(PublicAuxiliaryFunctionEnum.ADAPTER_VID_TYPE_OPTION.getKey()))) {
                    vidTypeList.addAll(JSONArray.parseArray(functionMap.get(PublicAuxiliaryFunctionEnum
                            .ADAPTER_VID_TYPE_OPTION.getKey()).toString(), Integer.class));
                }
            }
            if (CollectionUtils.isEmpty(filterVidType) || !Collections.disjoint(vidTypeList, filterVidType)) {
                resList.add(productVo);
            }
        });
        return resList;
    }

    @Override
    public ProductSelectForVidResVo productSelectForVid(ProductListForVidReqVo reqVo) {
        ProductSelectForVidResVo resVo = new ProductSelectForVidResVo();
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
        List<Integer> filterVidType = new ArrayList<>();
        if (Objects.nonNull(reqVo.getSelectVidType())) {
            filterVidType.add(reqVo.getSelectVidType());
            if (!DEFAULT_VID.equals(reqVo.getVid()) &&
                    (Objects.nonNull(rootNode) && rootNode.getVidType().equals(reqVo.getSelectVidType()))) {
                filterVidType.add(DEFAULT_VID_TYPE);
            }
        }

        List<productListVo> productList = productListForVid(reqVo);
        List<Long> marketingProductIds = mainContentProperties.getMarketingProductIds();
        List<Long> filterProductIds = mainContentProperties.getFilterProductIds();
        Boolean roleScoping = Boolean.FALSE;
        if (Objects.nonNull(reqVo.getRoleId())) {
            List<RoleInfoADTO> roleInfoADTOList = roleInfoInfoIntegrationService.getRoleInfoListByRoleIds(reqVo.getBosId(), reqVo.getRoleId());
            if (CollectionUtils.isEmpty(roleInfoADTOList)) {
                throw new ServerException("角色不存在");
            }

            RoleInfoADTO roleInfoADTO = roleInfoADTOList.get(0);
            //系统角色圈定范围
            if (StringUtils.isNotBlank(roleInfoADTO.getRoleCode()) && Objects.equals(roleInfoADTO.getRoleType(), RoleTypeEnum.SYSTEM_DEFAULT_NORMAL_ROLE.getRoleType())) {
                List<ProductRoleDTO> roleDTOList = productCenterIntegrationService.queryProductRoleInfoList(roleInfoADTO.getRoleCode());
                if (CollectionUtils.isNotEmpty(roleDTOList) && Objects.equals(roleDTOList.get(0).getUseConstraints(), 2)) {
                    ProductRoleProductIdQuery query = new ProductRoleProductIdQuery();
                    query.setRoleCode(roleInfoADTO.getRoleCode());
                    SoaResponse<List<Long>, Void> soaResponse = productRoleAbility.queryProductRoleProductIdList(query);
                    GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
                    List<Long> scopingProductList = soaResponse.getResponseVo();
                    if (CollectionUtils.isNotEmpty(scopingProductList)) {
                        // 查询角色下所有资源（跨应用）
                        List<ProductRoleVidTypeDTO> productRoleVidTypeDTOList = new ArrayList<>();
                        scopingProductList.forEach(productId -> {
                            ProductRoleVidTypeQuery productRoleVidTypeQuery = ProductRoleVidTypeQuery.builder().productId(productId).roleCode(roleInfoADTO.getRoleCode()).build();
                            List<ProductRoleVidTypeDTO> productRoleList = productCenterIntegrationService.queryProductRoleVidType(productRoleVidTypeQuery);
                            if (CollectionUtils.isNotEmpty(productRoleList)) {
                                productRoleVidTypeDTOList.addAll(productRoleList);
                            }
                        });
                        List<Long> vidTypeProductIdList = productRoleVidTypeDTOList.stream().filter(k ->
                                Objects.isNull(reqVo.getSelectVidType()) || filterVidType.contains(k.getVidType())).map(ProductRoleVidTypeDTO::getProductId).collect(toList());
                        productList = productList.stream().filter(product -> vidTypeProductIdList.contains(product.getProductId())).collect(toList());
                    }
                    roleScoping = Boolean.TRUE;
                }
            }

        }
        //过滤不可分配权限的应用
        if (!roleScoping && reqVo.getFilterProduct() && CollectionUtils.isNotEmpty(filterProductIds)) {
            productList = productList.stream().filter(product -> !filterProductIds.contains(product.getProductId())).collect(toList());
        }
        Map<ProductGroupDto, List<productListVo>> productGroupMap = productList.stream()
                .collect(Collectors.groupingBy(product -> {
                    if (marketingProductIds.contains(product.getProductId())) {
                        return new ProductGroupDto(Constants.MARKETING_PRODUCT_ID, mainContentProperties.getMarketingProductName());
                    }
                    return new ProductGroupDto(product.getProductId(), product.getProductName());
                }));
        List<ProductGroupDto> productGroupList = new ArrayList<>();
        productGroupMap.forEach((group, products) -> {
            //营销产品需要定制排序
            if (Objects.equals(Constants.MARKETING_PRODUCT_ID, group.getProductGroupId())) {
                products = products.stream().sorted(
                        Comparator.comparing(l -> marketingProductIds.indexOf(l.getProductId()),
                                Comparator.nullsLast(Integer::compareTo))
                ).collect(Collectors.toList());
            }
            group.setProductList(products);
            productGroupList.add(group);
        });
        productGroupList.sort(Comparator.comparing(ProductGroupDto::getProductGroupId));
        resVo.setProductGroupList(productGroupList);
        return resVo;
    }

    @Override
    public HttpResponse<PageScrollDto<AccountSelectResVo>> accountSelect(AccountSelectReqVo reqVo) {
        String searchKey = reqVo.getSearchKey();
        String accountName = null;
        List<Long> wids = new ArrayList<>();
        if (StringUtils.isNotBlank(searchKey)) {
            Pattern p = Pattern.compile("^1\\d{10}");
            Matcher m = p.matcher(searchKey);
            if (m.matches()) {
                Long wid = accountService.phoneToWid(null, searchKey, reqVo.getBosId());
                if (wid != null) {
                    wids.add(wid);
                }
            } else {
                accountName = searchKey;
            }
        }
        AccountScrollDTO<AccountBriefInfoDTO> accountScrollDTO = privilegeFacade.queryAccountByVid(
                reqVo.getBosId(), reqVo.getVid(), reqVo.getLastId(), reqVo.getPageSize(), accountName, wids);
        List<AccountSelectResVo> resVos = new ArrayList<>();
        List<AccountBriefInfoDTO> accountBriefInfoDTOS = accountScrollDTO.getAccountList();
        if (CollectionUtils.isNotEmpty(accountBriefInfoDTOS)) {
            List<Long> widList = accountBriefInfoDTOS.stream().map(a -> a.getWid()).distinct().collect(toList());
            resVos = BeanCopyUtils.copy(accountBriefInfoDTOS, AccountSelectResVo.class);
            Map<Long, String> widPhoneMap = accountService.batchWidToPhoneWithoutZone(widList, reqVo.getBosId());
            for (AccountSelectResVo accountSelectResVo : resVos) {
                accountSelectResVo.setPhoneNumber(widPhoneMap.get(accountSelectResVo.getWid()));
            }
        }
        PageScrollDto<AccountSelectResVo> resVo = new PageScrollDto<>(accountScrollDTO.getLastId(), accountScrollDTO.getMore(), resVos);
        return SoaTransformUtil.success(resVo);
    }

    @Override
    public HttpResponse<PageInfo<AccountSelectResVo>> overPrivilegeAccountSelect(AccountSelectPageReqVo reqVo) {
        String searchKey = reqVo.getSearchKey();
        List<Long> wids = new ArrayList<>();
        if (StringUtils.isNotBlank(searchKey)) {
            Pattern p = Pattern.compile("^1\\d{10}");
            Matcher m = p.matcher(searchKey);
            if (m.matches()) {
                Long wid = accountService.phoneToWid(null, searchKey, reqVo.getBosId());
                if (wid != null) {
                    wids.add(wid);
                }
            }
        }
        AccountListPageDTO<AccountBriefInfoDTO> pageDTO
                = privilegeFacade.queryAccountByBosId(reqVo.getBosId(), wids, reqVo.getPageNum(), reqVo.getPageSize());
        PageInfo<AccountSelectResVo> pageInfo = new PageInfo<>();
        List<AccountBriefInfoDTO> accountList = pageDTO.getAccountList();
        if (CollectionUtils.isEmpty(accountList)) {
            return SoaTransformUtil.success(pageInfo);
        } else {
            List<Long> widList = accountList.stream().map(a -> a.getWid()).collect(toList());
            List<AccountSelectResVo> resVos = BeanCopyUtils.copy(accountList, AccountSelectResVo.class);
            Map<Long, String> widPhoneMap = accountService.batchWidToPhone(widList, reqVo.getBosId());
            for (AccountSelectResVo accountSelectResVo : resVos) {
                accountSelectResVo.setPhoneNumber(widPhoneMap.get(accountSelectResVo.getWid()));
            }
            Page page = pageDTO.getPage();
            pageInfo.setPageNumber(page.getPageNumber());
            pageInfo.setPageSize(page.getPageSize());
            pageInfo.setPageTotal(page.getPageTotal());
            pageInfo.setTotalCount(Long.valueOf(page.getTotalCount()));
            pageInfo.setData(resVos);
            return SoaTransformUtil.success(pageInfo);
        }
    }


    @Override
    public HttpResponse<String> generaterKey(GeneraterKeyReqVo reqVo) {
        Long productInstanceId = reqVo.getProductInstanceId();
        if (Objects.isNull(reqVo.getProductInstanceId())) {
            productInstanceId = Constants.DEFAULT_PRODUCT_INSTANCE_ID;
        }
        String key = KeyGenerater.getKeyStr(reqVo.getBosId(), productInstanceId, reqVo.getVid());
        return SoaTransformUtil.success(key);
    }

    @Override
    public HttpResponse<List<InstanceJumpUrlVo>> obtainInstanceJumpUrl(InstanceJumpUrlQueryVo instanceJumpUrlQueryVo) {
        Long bosId = instanceJumpUrlQueryVo.getBosId();
        List<Long> instanceIds = instanceJumpUrlQueryVo.getInstanceIds();
        Long wid = instanceJumpUrlQueryVo.getWid();
        Long vid = instanceJumpUrlQueryVo.getVid();
        Integer vidType = instanceJumpUrlQueryVo.getVidType();

        QueryProductInstanceIdShow queryProductInstanceIdShow = new QueryProductInstanceIdShow(wid, bosId,
                vid, vidType, instanceIds);
        SoaResponse<List<ProductInstanceIdShowDTO>, Void> listVoidSoaResponse = integrationExportService
                .queryProductInstanceIdShow(queryProductInstanceIdShow);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(listVoidSoaResponse), MpBizCode.build(listVoidSoaResponse));
        List<ProductInstanceIdShowDTO> instanceIdShowDTOS = listVoidSoaResponse.getResponseVo();

        List<ProductInstanceFlowDTO> productInstanceFlowDTOS = productFacade
                .queryProductInstanceListByInstanceIds(instanceIds);

        Map<Long, ProductInstanceFlowDTO> instanceFlowDTOMap = productInstanceFlowDTOS.stream()
                .collect(Collectors.toMap(ProductInstanceFlowDTO::getProductInstanceId, Function.identity(), (v1,
                                                                                                              v2) -> v2));
        List<InstanceJumpUrlVo> collect = instanceIdShowDTOS.stream().map(e -> {
            InstanceJumpUrlVo instanceJumpUrlVo = new InstanceJumpUrlVo();
            instanceJumpUrlVo.setHasUsePermission(e.getShow());
            if (BooleanUtil.isTrue(e.getShow())) {
                ProductInstanceFlowDTO instanceFlowDTO = instanceFlowDTOMap.get(e.getProductInstanceId());
                if (Objects.nonNull(instanceFlowDTO)) {
                    String jumpUrl = instanceFlowDTO.getJumpUrl();
                    String parsedUrl = productFacade.transform(jumpUrl, bosId, e.getProductInstanceId(), vid);
                    instanceJumpUrlVo.setJumpUrl(parsedUrl);
                }
            }
            instanceJumpUrlVo.setInstanceId(e.getProductInstanceId());
            return instanceJumpUrlVo;
        }).collect(toList());
        return SoaTransformUtil.success(collect);

    }

    @Override
    public HttpResponse<List<InstanceOpenStateVo>> instanceOpenState(InstanceOpenStateQueryVo instanceOpenStateQueryVo) {

        List<Long> instanceIds = instanceOpenStateQueryVo.getInstanceIds();
        List<InstanceOpenStateVo> res = initRes(instanceIds);
        List<ProductInstanceId> productInstanceIds = instanceIds.stream()
                .map(e -> new ProductInstanceId(e)).collect(toList());
        /**
         * 批量查询instanceIds对应的适配节点类型附属功能
         */
        AttachedFunctionBatchQuery functionBatchQuery = new AttachedFunctionBatchQuery(productInstanceIds);
        functionBatchQuery.setKeys(Arrays.asList(ADAPTER_VID_TYPE_OPTION.getKey()));
        List<AttachedFunctionDTO> attachedFunctionDTOList = productFacade.batchQueryAttachedFunction(functionBatchQuery);
        if (CollectionUtils.isEmpty(attachedFunctionDTOList)) {
            return SoaTransformUtil.success(res);
        }
        Map<ProductInstanceId, AttachedFunctionDTO> instanceId2AttachedFunctions = attachedFunctionDTOList.stream().collect(
                Collectors.toMap(AttachedFunctionDTO::getInstanceId, Function.identity(),
                        (existing, replacement) -> existing));

        List<ProductInstanceFlowDTO> instanceFlowDTOList = productFacade
                .queryProductInstanceListByInstanceIds(instanceIds);
        for (Long instanceId : instanceIds) {
            //适用组织类型组装
            AttachedFunctionDTO attachedFunctionDTO = instanceId2AttachedFunctions
                    .get(new ProductInstanceId(instanceId));
            List<Integer> adaptiveVidTypeCodeList = new ArrayList<>();
            if (Objects.nonNull(attachedFunctionDTO)) {
                adaptiveVidTypeCodeList = objToList(attachedFunctionDTO.getFunctions().get(ADAPTER_VID_TYPE_OPTION.getKey()));
            }
            if (adaptiveVidTypeCodeList.contains(VidTypeEnum.NULL_TYPE.getType())) {
                res.forEach(e -> {
                    if (instanceId.equals(e.getInstanceId())) {
                        e.setOpened(true);
                    }
                });
            } else {
                //不适配店铺节点
                Optional<ProductInstanceFlowDTO> instanceFlowDTO = instanceFlowDTOList.stream()
                        .filter(e -> e.getProductInstanceId().equals(instanceId)).findFirst();
                boolean present = instanceFlowDTO.isPresent();
                if (present) {
                    ProductInstanceFlowDTO instance = instanceFlowDTO.get();
                    Integer sumOpedCount = sumOpedCount(instance, adaptiveVidTypeCodeList);
                    if (sumOpedCount > 0) {
                        res.forEach(e -> {
                            if (instanceId.equals(e.getInstanceId())) {
                                e.setOpened(true);
                            }
                        });
                    }
                }
            }
        }
        return SoaTransformUtil.success(res);
    }


    private Integer sumOpedCount(ProductInstanceFlowDTO instance, List<Integer> adaptiveVidType) {
        int sum = 0;
        for (Integer vidType : adaptiveVidType) {
            if (VidTypeEnum.BRAND.getType().equals(vidType)) {
                Integer openBrandNum = instance.getOpenBrandNum();
                sum += (Objects.isNull(openBrandNum) ? 0 : openBrandNum);
            } else if (VidTypeEnum.GROUP.getType().equals(vidType)) {
                Integer openGroupNum = instance.getOpenGroupNum();
                sum += (Objects.isNull(openGroupNum) ? 0 : openGroupNum);
            } else if (VidTypeEnum.DEPARTMENT.getType().equals(vidType)) {
                Integer openDepartmentNum = instance.getOpenDepartmentNum();
                sum += (Objects.isNull(openDepartmentNum) ? 0 : openDepartmentNum);
            } else if (VidTypeEnum.OUTLETS.getType().equals(vidType)) {
                Integer openOutletsNum = instance.getOpenOutletsNum();
                sum += (Objects.isNull(openOutletsNum) ? 0 : openOutletsNum);
            } else if (VidTypeEnum.DISTRICT.getType().equals(vidType)) {
                Integer openDistrictNum = instance.getOpenDistrictNum();
                sum += (Objects.isNull(openDistrictNum) ? 0 : openDistrictNum);
            } else if (VidTypeEnum.MALL.getType().equals(vidType)) {
                Integer openMallNum = instance.getOpenMallNum();
                sum += (Objects.isNull(openMallNum) ? 0 : openMallNum);
            } else if (VidTypeEnum.FLOOR.getType().equals(vidType)) {
                Integer openFloorNum = instance.getOpenFloorNum();
                sum += (Objects.isNull(openFloorNum) ? 0 : openFloorNum);
            } else if (VidTypeEnum.STORE.getType().equals(vidType)) {
                Integer openStoreNum = instance.getOpenStoreNum();
                sum += (Objects.isNull(openStoreNum) ? 0 : openStoreNum);
            } else if (VidTypeEnum.SELF_PICKUP_SITE.getType().equals(vidType)) {
                Integer openSelfPickUpSiteNum = instance.getOpenSelfPickUpSiteNum();
                sum += (Objects.isNull(openSelfPickUpSiteNum) ? 0 : openSelfPickUpSiteNum);
            }
        }
        return sum;
    }


    private List<InstanceOpenStateVo> initRes(List<Long> instanceIds) {
        List<InstanceOpenStateVo> collect = instanceIds.stream().map(e -> new InstanceOpenStateVo(e, false))
                .collect(toList());
        return collect;
    }

    @Override
    public HttpResponse<InstanceJumpUrlVo> checkProductManage(CheckProductManageVo checkProductManageVo) {
        Long bosId = checkProductManageVo.getBosId();
        Long wid = checkProductManageVo.getWid();
        InstanceJumpUrlVo instanceJumpUrlVo = new InstanceJumpUrlVo();
        instanceJumpUrlVo.setHasUsePermission(false);
        List<ProductInstanceFlowDTO> instanceFlowDTOS = productFacade.getBosProductInstanceId(bosId);
        Long baseProductInstanceId = null;
        for (ProductInstanceFlowDTO instanceFlowDTO : instanceFlowDTOS) {
            if (Constants.DEFAULT_PRODUCT_ID.equals(instanceFlowDTO.getProductId())) {
                baseProductInstanceId = instanceFlowDTO.getProductInstanceId();
            }
        }
        if (Objects.isNull(baseProductInstanceId)) {
            return SoaTransformUtil.success(instanceJumpUrlVo);
        }
        String productManageMenuCode = null;
        boolean menuFound = false;
        MenuItem menuItem = null;
        List<MenuDTO> menuDTOS = productFacade
                .getMenuListByProductInstance(Arrays.asList(baseProductInstanceId), SystemChannelEnum.PC,
                        AppTypeEnum.ALL);
        for (MenuDTO menuDTO : menuDTOS) {
            if (menuDTO.getInstanceId().getId().equals(baseProductInstanceId)) {
                if (CollectionUtils.isNotEmpty(menuDTO.getMenus())) {
                    for (MenuItem menu : menuDTO.getMenus()) {
                        if (Objects.nonNull(productManageMenuId) && productManageMenuId.equals(menu.getMenuId())) {
                            menuFound = true;
                            menuItem = menu;
                            productManageMenuCode = menu.getAuthCode();
                        }
                    }
                }
            }
        }
        if (menuFound) {
            String parsedUrl = productFacade
                    .transform(menuItem.getUrl(), bosId, baseProductInstanceId, Constants.DEFAULT_VID);
            if (StringUtils.isBlank(productManageMenuCode)) {
                instanceJumpUrlVo.setHasUsePermission(true);
                instanceJumpUrlVo.setJumpUrl(parsedUrl);
                return SoaTransformUtil.success(instanceJumpUrlVo);
            } else {
                //调用权限接口查询
                QueryCodeListShowRequest showRequest = new QueryCodeListShowRequest(wid, bosId, Constants.DEFAULT_VID,
                        VidTypeEnum.NULL_TYPE.getType(), baseProductInstanceId, Arrays.asList(productManageMenuCode));
                SoaResponse<List<CodeShowDTO>, Void> listVoidSoaResponse = integrationExportService
                        .queryCodeListShow(showRequest);
                GEPreconditions.checkState(SoaRespUtils.isSuccess(listVoidSoaResponse), MpBizCode.build(listVoidSoaResponse));
                List<CodeShowDTO> responseVo = listVoidSoaResponse.getResponseVo();
                if (CollectionUtils.isEmpty(responseVo)) {
                    return SoaTransformUtil.success(instanceJumpUrlVo);
                }
                for (CodeShowDTO codeShowDTO : responseVo) {
                    if (productManageMenuCode.equals(codeShowDTO.getCode())) {
                        instanceJumpUrlVo.setHasUsePermission(codeShowDTO.getShow());
                        if (codeShowDTO.getShow()) {
                            instanceJumpUrlVo.setJumpUrl(parsedUrl);
                        }
                        return SoaTransformUtil.success(instanceJumpUrlVo);
                    }
                }
                return SoaTransformUtil.success(instanceJumpUrlVo);
            }

        } else {
            return SoaTransformUtil.success(instanceJumpUrlVo);
        }
    }

    @Override
    public HttpResponse<PIIdForVidResVo> queryPIIdForVid(QueryPIIdForVidReqVo reqVo) {
        List<ProductInstanceFlowDTO> productInstanceIds;
        String key;
        Long productInstanceId;
        PIIdForVidResVo resVo = new PIIdForVidResVo();
        if (Objects.nonNull(reqVo.getVid())) {
            if (Objects.nonNull(reqVo.getProductId())) {
                if (Constants.DEFAULT_VID.equals(reqVo.getVid())) {
                    productInstanceIds = productFacade.getBosProductInstanceId(reqVo.getBosId());
                } else {
                    productInstanceIds = productFacade.getVidProductInstanceId(reqVo.getVid());
                }
                ProductInstanceFlowDTO productInstanceFlowDTO = productInstanceIds.stream()
                        .filter(instans -> instans.getProductId().equals(reqVo.getProductId()))
                        .findAny().orElse(null);
                if (Objects.isNull(productInstanceFlowDTO)) {
                    throw new ServerException("节点未开通此产品");
                }
                productInstanceId = productInstanceFlowDTO.getProductInstanceId();
                BeanCopyUtils.copy(productInstanceFlowDTO, resVo);
            } else {
                productInstanceId = Constants.DEFAULT_PRODUCT_INSTANCE_ID;
            }
            key = KeyGenerater.getKeyStr(reqVo.getBosId(), productInstanceId, reqVo.getVid());
        } else {
            key = KeyGenerater.getKeyStr(reqVo.getBosId(), Constants.DEFAULT_PRODUCT_INSTANCE_ID, null);
            productInstanceId = Constants.DEFAULT_PRODUCT_INSTANCE_ID;
        }
        resVo.setKey(key);
        resVo.setProductInstanceId(productInstanceId);
        return SoaTransformUtil.success(resVo);
    }

    @Override
    public HttpResponse<CodeListByProductIdResVo> queryCodeListByProductId(CodeListByProductIdReqVo reqVo) {
        List<ProductInstanceFlowDTO> productInstanceIds;
        if (Constants.DEFAULT_VID.equals(reqVo.getVid())) {
            productInstanceIds = productFacade.getBosProductInstanceId(reqVo.getBosId());
        } else {
            productInstanceIds = productFacade.getVidProductInstanceId(reqVo.getVid());
        }
        ProductInstanceFlowDTO productInstanceFlowDTO = productInstanceIds.stream()
                .filter(instance -> instance.getProductId().equals(reqVo.getProductId()))
                .findAny().orElse(null);
        if (Objects.isNull(productInstanceFlowDTO)) {
            throw new ServerException("节点未开通此产品");
        }
        Long productInstanceId = productInstanceFlowDTO.getProductInstanceId();
        List<String> codeList = privilegeFacade
                .queryCodeList(reqVo.getWid(), reqVo.getBosId(), reqVo.getVid(), reqVo.getVidType(),
                        SceneItemValueEnum.CHANNEL_PC.getItemValue(), productInstanceId);

        CodeListByProductIdResVo resVo = new CodeListByProductIdResVo();
        resVo.setCodeList(codeList);
        return SoaTransformUtil.success(resVo);
    }

    @Override
    public HttpResponse<ProductVersionListVo> versionList(VersionListReqVo reqVo) {

        ProductVersionQueryReqDTO versionQueryReqDTO = new ProductVersionQueryReqDTO();
        versionQueryReqDTO.setProductIdList(Arrays.asList(reqVo.getProductId()));
        versionQueryReqDTO.setProductVersionIdList(Arrays.asList(reqVo.getVersionId()));
        SoaResponse<List<ProductVersionDTO>, ErrInfo> listErrInfoSoaResponse = productVersionManagerAbility
                .productVersionList(versionQueryReqDTO);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(listErrInfoSoaResponse), MpBizCode.build(listErrInfoSoaResponse));
        List<ProductVersionDTO> responseVo = listErrInfoSoaResponse.getResponseVo();
        ProductVersionListVo versionListVo = new ProductVersionListVo();
        if (CollectionUtils.isEmpty(responseVo)) {
            return SoaTransformUtil.success(versionListVo);
        }
        String productName = responseVo.get(0).getProductName();
        versionListVo.setProductName(productName);

        List<ProductVersionDto> productVersionDtos = BeanCopyUtils.copy(responseVo, ProductVersionDto.class);
        versionListVo.setProductVersionList(productVersionDtos);
        return SoaTransformUtil.success(versionListVo);
    }

    @Override
    public HttpResponse<List<PhoneZoneInfoResponse>> getAllZones() {
        SoaResponse<List<PhoneZoneInfoResponse>, Void> soaResponse = accountZonesAbility.getAllZones(null);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        return SoaTransformUtil.build(soaResponse);
    }

    @Override
    public HttpResponse<List<RoleInfoDto>> selectRoleList(SelectRoleListReqVo reqVo) {
        QueryRoleByCreateVidAndSceneRequest request = BeanCopyUtils.copy(reqVo, QueryRoleByCreateVidAndSceneRequest.class);
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
        if (rootNode != null && (Objects.equals(reqVo.getCreateVid(), 0L) || Objects.equals(reqVo.getCreateVid(), rootNode.getVid()))) {
            request.setCreateVidList(new ArrayList<>());
            request.getCreateVidList().add(0L);
            request.getCreateVidList().add(rootNode.getVid());
        }
        List<SceneItemDTO> sceneItemDTOS = privilegeFacade
                .getSceneItemDTOS(null, null, SceneItemValueEnum.ALL);
        request.setSceneItemDTOList(sceneItemDTOS);
        request.setPageSize(reqVo.getPageSize());
        request.setLastId(reqVo.getLastRoleId());
        SoaResponse<RollPageDTO<RoleInfoDTO>, Void> soaResponse = roleInfoExportService
                .queryRoleByCreateVidAndScene(request);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        List<RoleInfoDTO> roleList = soaResponse.getResponseVo().getData();
        roleList = roleList.stream().filter(roleInfoDTO -> {
            return !(Objects.nonNull(rootNode) && Objects.equals(roleInfoDTO.getRoleType(), RoleTypeEnum.SYSTEM_DEFAULT_SUPER_ROLE.getRoleType()) && Objects.equals(roleInfoDTO.getCreateVid(), rootNode.getVid()));
        }).collect(toList());
        List<Long> vidList = roleList.stream().map(RoleInfoDTO::getCreateVid)
                .filter(vid -> Objects.nonNull(vid) && !Constants.DEFAULT_VID.equals(vid)).distinct().collect(toList());
        Map<Long, com.weimob.mp.merchantstructure.common.domain.common.VidInfoDto> vidMap = structureFacade
                .getVidMap(vidList);
        String bosName = structureFacade.getDefaultBosName(reqVo.getBosId());
        List<RoleInfoDto> resList = new ArrayList<>();
        EnhancedBeanCopyUtils.copy(roleList, resList, RoleInfoDto.class, (o1, o2) ->
                o2.setCreateVidName(Objects.nonNull(vidMap.get(o1.getCreateVid())) ?
                        vidMap.get(o1.getCreateVid()).getBaseInfo().getVidName() :
                        (Constants.DEFAULT_VID.equals(o1.getCreateVid()) ? bosName : null)));
        return SoaTransformUtil.success(resList);
    }


    @Override
    public HttpResponse<Boolean> subscribe(SubscribeReqVo subscribeReqVo) {
        ContactReqDTO contactReqDTO = BeanCopyUtils.copy(subscribeReqVo, ContactReqDTO.class);

        List<SubscribeSourceProperties.NpSource> sources = subscribeSourceProperties.getSources();
        List<SourceDTO> sourceDTOS = BeanCopyUtils.copy(sources, SourceDTO.class);

        PurposeReqDTO purposeReqDTO = PurposeReqDTO.builder()
                .productId(subscribeReqVo.getProductId())
                .productName(subscribeReqVo.getProductName())
                .productVersion(String.valueOf(subscribeReqVo.getVersionId()))
                .productVersionName(subscribeReqVo.getProductVersionName())
                .buyType(subscribeReqVo.getBuyType())
                .urgentType(UrgentTypeEnum.URGENT.getType())
                .sourceDTOList(sourceDTOS)
                .build();
        CreateSingleClueDTO singleClueDTO = CreateSingleClueDTO.builder()
                .contactResDTOList(Arrays.asList(contactReqDTO))   //联系人
                .purposeReqDTOList(Arrays.asList(purposeReqDTO))
                .bosId(subscribeReqVo.getBosId())
                .sourceType(SourceTypeEnum.MC.getType())
                .build();
        SoaResponse<CreateSingleClueResponseDTO, ErrInfo> singleClue =
                clueFacade.createSingleClue(singleClueDTO);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(singleClue), MpBizCode.build(singleClue));
        return SoaTransformUtil.success(true);
    }

    /**
     * 记录最近访问的节点
     *
     * @param lastActiveNodeRecordDto
     * @return
     */
    @Override
    public HttpResponse<Boolean> recordLastActiveNode(LastActiveNodeRecordDto lastActiveNodeRecordDto) {
        Long bosId = lastActiveNodeRecordDto.getBosId();
        Long vid = lastActiveNodeRecordDto.getVid();
        Long wid = lastActiveNodeRecordDto.getWid();

        String vidStr = mpMerchantRedisService.get(Constants.VID_SWITCHING_PREFIX + wid + bosId);
        if (StringUtils.isNotBlank(vidStr)) {
            if (!Long.valueOf(vidStr).equals(vid)) {
                mpMerchantRedisService.set(Constants.VID_SWITCHING_PREFIX + wid + bosId, String.valueOf(vid));
            }
        } else {
            mpMerchantRedisService.set(Constants.VID_SWITCHING_PREFIX + wid + bosId, String.valueOf(vid));
        }

        return SoaTransformUtil.success(true);
    }

    @Override
    public HttpResponse<UDeskInfoResVo> uDeskInfo(UDeskInfoReqVo reqVo) {
        UDeskInfoResVo authData = getAuthData(reqVo.getBosId());
        List<UDeskInfoResVo.DetailInfo> extendInfos = new ArrayList<>();
        UDeskInfoResVo.DetailInfo info1 = UDeskInfoResVo.DetailInfo.builder()
                .displayName("微盟账号")
                .data(accountService.widAndBosIdToPhone(reqVo.getWid(), reqVo.getBosId()).getPhone()).build();
        extendInfos.add(info1);
        List<ProductInstanceFlowDTO> bosProductInstanceId = productFacade.getBosProductInstanceId(reqVo.getBosId());
        StringBuilder sb = new StringBuilder();
        for (ProductInstanceFlowDTO productInstanceFlowDTO : bosProductInstanceId) {
            sb.append(productInstanceFlowDTO.getVersionName()).append("、");
        }
        UDeskInfoResVo.DetailInfo info2 = UDeskInfoResVo.DetailInfo.builder()
                .displayName("已购应用")
                .data(sb.substring(0, sb.length() - 1)).build();
        extendInfos.add(info2);
        UDeskInfoResVo.DetailInfo info3 = UDeskInfoResVo.DetailInfo.builder()
                .displayName("bos_id")
                .data(reqVo.getBosId()).build();
        extendInfos.add(info3);
        String agentName = "";
        CheckCustomerSourceRequest request = new CheckCustomerSourceRequest();
        request.setPid(reqVo.getBosId());
        SoaResponse<CheckCustomerSourceResponse, NoneError> customSourceResponse = remoteService.checkCustomerSource(request);
        if (SoaRespUtils.isSuccess(customSourceResponse)) {
            CheckCustomerSourceResponse source = customSourceResponse.getResponseVo();
            if (source != null) {
                agentName = source.getAgentName();
                if (StringUtils.isEmpty(agentName)) {
                    agentName = source.getCustomerSource();
                }
            }
        }
        UDeskInfoResVo.DetailInfo info4 = UDeskInfoResVo.DetailInfo.builder()
                .displayName("代理商名称")
                .data(agentName).build();
        extendInfos.add(info4);
        authData.setExtendInfos(extendInfos);
        return SoaTransformUtil.success(authData);
    }

    private final String baseSignStr = "nonce=%s&timestamp=%s&web_token=%s&%s";

    private UDeskInfoResVo getAuthData(Long bosId) {
        Random random = new Random();
        String nonce = Integer.toString(Math.abs(random.nextInt()));
        String timestamp = Long.toString(System.currentTimeMillis());
        String webToken = Long.toString(bosId);
        String str = String.format(baseSignStr, nonce, timestamp, webToken, imUserKey);
        String signature = SHA1WithUpper(str);
        return UDeskInfoResVo.builder()
                .nonce(nonce)
                .signature(signature)
                .timestamp(timestamp)
                .web_token(webToken)
                .build();
    }

    /**
     * 故障降级方法
     *
     * @param reqVo
     * @return
     */
    @ErrorWrapper
    public HttpResponse<KeyOrMenuResVo> fallbackHandler(CommonKeyReqDto reqVo) {
        String globalTicket = ZipkinContext.getContext().getGlobalTicket();
        Map<String, Object> map = Maps.newHashMap();
        map.put("errTicketId", globalTicket);
        Cat.logTransaction("fallback", "fallbackHandler.queryCacheableMenus",
                System.currentTimeMillis(),
                Message.FAIL, map);
        KeyOrMenuResVo l2CacheMenus = menusMultiCacheManager.getL2CacheMenus(reqVo.getWid(), reqVo.getKey());
        if (Objects.isNull(l2CacheMenus)) {
            KeyOrMenuResVo keyOrMenuResVo = new KeyOrMenuResVo();
            keyOrMenuResVo.setMenus(Arrays.asList());
            keyOrMenuResVo.setCodeList(Arrays.asList());
            return SoaTransformUtil.success(keyOrMenuResVo);
        }
        return SoaTransformUtil.success(l2CacheMenus);
    }


    @Override
    @SentinelResource(value = "queryCacheableMenus", fallback = "fallbackHandler")
    public HttpResponse<KeyOrMenuResVo> queryCacheableMenus(CommonKeyReqDto reqVo) {
        Long wid = reqVo.getWid();

        String key = reqVo.getKey();
        Map<String, Long> compressKey = KeyGenerater.unCompressKey(key);
        if (Objects.isNull(compressKey)) {
            return SoaTransformUtil.success(new KeyOrMenuResVo());
        }
        Long bosId = compressKey.get(KeyGenerater.BOSID);
        Long vid = compressKey.get(KeyGenerater.VID);
        Long instanceId = compressKey.get(KeyGenerater.PRODUCT_INSTANCE_ID);

        Integer vidType = null;
        if (Objects.nonNull(vid)) {
            QueryVidInfoResponseDto vidInfo = structureFacade.getVidInfo(vid);
            if (Objects.isNull(vidInfo) && vid.equals(DEFAULT_VID)) {
                vidType = DEFAULT_VID_TYPE;
            } else if (CollectionUtils.isNotEmpty(vidInfo.getVidTypes())) {
                vidType = vidInfo.getVidTypes().get(0);
            }
        } else {
            vid = DEFAULT_VID;
            vidType = DEFAULT_VID_TYPE;
        }

        KeyOrMenuResVo keyOrMenuResVo = menusMultiCacheManager.getL1CacheMenus(wid, key);
        if (Objects.nonNull(keyOrMenuResVo)) {
            return SoaTransformUtil.success(keyOrMenuResVo);
        }

        QueryForKeyOrMenuReqVo keyOrMenuReqVo = new QueryForKeyOrMenuReqVo();
        keyOrMenuReqVo.setBosId(bosId);
        keyOrMenuReqVo.setChannel("ALL");
        keyOrMenuReqVo.setProductInstanceId(instanceId);
        keyOrMenuReqVo.setVid(vid);
        keyOrMenuReqVo.setVidType(vidType);
        keyOrMenuReqVo.setWid(wid);
        HttpResponse<KeyOrMenuResVo> keyOrMenuResVoHttpResponse = this.queryForKeyOrMenuV2(keyOrMenuReqVo);
        KeyOrMenuResVo menuResVo = keyOrMenuResVoHttpResponse.getData();
        GEPreconditions.checkState(menuResVo != null, MpBizCode.CODE_IDENTIFY_NO_PROVIDER);

        if (Objects.nonNull(menuResVo)) {
            menusMultiCacheManager.setL1AndL2CacheMenus(wid, key, menuResVo);
        }
        return SoaTransformUtil.success(menuResVo);
    }

    @Override
    public HttpResponse<PageScrollDto<VidInfoDto>> querySelectVids(QuerySelectVidsReqVo reqVo) {
        BusinessOsDto bos = Objects.requireNonNull(structureFacade.getBosInfo(reqVo.getBosId()));
        PageScrollDto<VidInfoDto> pageScrollDto = new PageScrollDto<>();
        pageScrollDto.setMore(false);
        pageScrollDto.setScrollId(null);
        if (DEFAULT_VID.equals(reqVo.getVid())) {
            VidInfoDto vidInfoDto = new VidInfoDto();
            vidInfoDto.setBosId(reqVo.getBosId());
            vidInfoDto.setVid(DEFAULT_VID);
            vidInfoDto.setVidType(Constants.DEFAULT_VID_TYPE);
            vidInfoDto.setVidName(bos.getBosName());
            pageScrollDto.setDataList(Arrays.asList(vidInfoDto));
            return SoaTransformUtil.success(pageScrollDto);
        }

        QueryVidPageByScrollReqDTO request = new QueryVidPageByScrollReqDTO();
        request.setSize(reqVo.getPageSize());
        request.setScrollId(reqVo.getScrollId());
        request.setBosId(reqVo.getBosId());
        request.setParentVid(reqVo.getVid());
        request.setVidTypes(Arrays.asList(reqVo.getSelectVidType()));
        request.setDepthLayer(Boolean.TRUE);
        request.setResultData(Arrays.asList(NodeSearchParentNodeEnum.PARENT_NODE.getCode()));
        request.setVidName(reqVo.getVidName());
        SoaResponse<VidPageRespDTO, Void> soaResponse = nodeSearchExportService.queryVidPageByScroll(request);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));

        List<VidPageInfoDTO> vidInfoDTOS = soaResponse.getResponseVo().getVidInfos();
        List<VidInfoDto> resList = new ArrayList<>();
        if (CollectionUtils.isEmpty(vidInfoDTOS)) {
            pageScrollDto.setDataList(Collections.emptyList());
            return SoaTransformUtil.success(pageScrollDto);
        }

        if (Objects.nonNull(reqVo.getSelectVid())) {
            vidInfoDTOS = vidInfoDTOS.stream().filter(info -> !info.getVid().equals(reqVo.getSelectVid())).collect(toList());
            if (StringUtils.isBlank(reqVo.getScrollId())) {
                QueryVidInfoResponseDto vidInfo = structureFacade.getVidInfo(reqVo.getSelectVid());
                VidPageInfoDTO vidInfoDto = BeanCopyUtils.copy(vidInfo, VidPageInfoDTO.class);
                vidInfoDTOS.add(0, vidInfoDto);
            }
        }
        List<Long> parentVidList = vidInfoDTOS.stream().map(VidPageInfoDTO::getParentVid)
                .filter(id -> Objects.nonNull(id) && !Constants.DEFAULT_VID.equals(id))
                .distinct().collect(toList());
        Map<Long, NodeBaseInfoDto> baseInfoDtoMap = structureFacade.selectVidName(parentVidList);
        for (VidPageInfoDTO vidInfo : vidInfoDTOS) {
            VidInfoDto vidInfoDto = new VidInfoDto();
            vidInfoDto.setBosId(reqVo.getBosId());
            vidInfoDto.setVid(vidInfo.getVid());
            vidInfoDto.setVidType(vidInfo.getVidTypes().get(0));
            vidInfoDto.setVidName(vidInfo.getBaseInfo().getVidName());
            Integer vidStatus = vidInfo.getBaseInfo().getVidStatus();
            vidInfoDto.setVidStatus(vidStatus);
            if (Objects.nonNull(vidStatus) && VidStatusEnum.DISABLE.getId().equals(vidStatus)) {
                vidInfoDto.setVidName(vidInfoDto.getVidName() + "(已停用)");
            }
            vidInfoDto.setParentVid(vidInfo.getParentVid());
            if (Objects.nonNull(vidInfo.getParentVid())) {
                vidInfoDto.setParentVidName(Constants.DEFAULT_VID.equals(vidInfoDto.getParentVid()) ? bos.getBosName() :
                        baseInfoDtoMap.get(vidInfoDto.getParentVid()).getVidName());
            }
            resList.add(vidInfoDto);
        }
        pageScrollDto.setScrollId(soaResponse.getResponseVo().getScrollId());
        pageScrollDto.setMore(soaResponse.getResponseVo().getMore());
        pageScrollDto.setDataList(resList);
        return SoaTransformUtil.success(pageScrollDto);
    }

    @Override
    public HttpResponse<KeyOrMenuResVo> queryForKeyOrMenuV2(QueryForKeyOrMenuReqVo reqVo) {
        Long productInstanceId = reqVo.getProductInstanceId();
        Long currentVid = reqVo.getVid();

        if (Objects.nonNull(productInstanceId) && productInstanceId.equals(0L)) {
            return SoaTransformUtil.success(new KeyOrMenuResVo());
        }

        Long currentNode = currentVid;
        Long bosId = reqVo.getBosId();
        Long wid = reqVo.getWid();
        Integer vidType = reqVo.getVidType();
        List<String> codeList;
        boolean blendNode = structureFacade.isBlendNode(currentNode);

        SoaResponse<ProductInstanceFlowDTO, Void> flowDTOVoidSoaResponse = productFlowApi
                .queryProductInstanceByInstanceId(
                        new ProductInstanceDetailFlowQuery(new ProductInstanceId(productInstanceId)));
        GEPreconditions.checkState(SoaRespUtils.isSuccess(flowDTOVoidSoaResponse), MpBizCode.build(flowDTOVoidSoaResponse));
        ProductInstanceFlowDTO flowDTO = flowDTOVoidSoaResponse.getResponseVo();
        Long productId = flowDTO.getProductId();

        /**
         * 处理topbar多应用融合场景
         */
        QueryTopBarEntryListRequest entryListRequest = QueryTopBarEntryListRequest.builder()
                .productIds(Arrays.asList(productId))
                .build();
        SoaResponse<List<TopBarEntryDTO>, Void> listVoidSoaResponse = topBarEntryAbility
                .queryTopBarEntryListV2(entryListRequest);
        GEPreconditions.checkState(SoaRespUtils.isSuccess(listVoidSoaResponse), MpBizCode.build(listVoidSoaResponse));
        List<TopBarEntryDTO> entryDTOList = listVoidSoaResponse.getResponseVo();
        if (CollectionUtils.isNotEmpty(entryDTOList)) {
            List<TopBarEntryDTO> productList = entryDTOList.stream()
                    .filter(e -> e.getEntryType().equals(TopBarEntryType.PRODUCT.getIndex())).collect(
                            Collectors.toList());
            if (CollectionUtils.isNotEmpty(productList)) {
                //应用管理后台规定一个productId只能关联一个产品型topbar
                TopBarEntryDTO entryDTO = productList.get(0);
                List<Long> productIds = entryDTO.getProductIds();
                if (CollectionUtils.isNotEmpty(productIds) && productIds.size() > 1) {
                    List<ProductInstanceFlowDTO> instances;
                    if (Objects.equals(currentVid, 0L)) {
                        instances = productFacade.getBosProductInstanceId(bosId);
                    } else {
                        instances = productFacade.getVidProductInstanceId(currentVid);
                    }
                    List<ProductInstanceFlowDTO> instanceIds = instances.stream()
                            .filter(e -> productIds.contains(e.getProductId())).collect(toList());
                    List<CompletableFuture<KeyOrMenuResVo>> completableFutures = new ArrayList<>();
                    for (ProductInstanceFlowDTO instanceFlowDTO : instanceIds) {
                        CompletableFuture<KeyOrMenuResVo> asyncCompletable = asyncService
                                .exeAsyncCompletable(() -> {
                                    Long proId = instanceFlowDTO.getProductId();
                                    Long instanceId = instanceFlowDTO.getProductInstanceId();
                                    List<String> privilegeCodes = getPrivilegeCodes(instanceId, currentNode,
                                            bosId, wid, vidType, blendNode);
                                    QueryPrivilegeTreeDto queryPrivilegeTreeDto = QueryPrivilegeTreeDto.builder()
                                            .appChecked(false)
                                            .bosId(bosId)
                                            .channelGroup(null)
                                            .channelScene(SceneItemValueEnum.CHANNEL_PC)
                                            .codeList(privilegeCodes)
                                            .instance(instanceFlowDTO)
                                            .markOrFilter(false)
                                            .menuTypeEnum(MenuTypeEnum.PC)
                                            .roleTypeEnum(null)
                                            .showModelEnum(null)
                                            .vid(currentNode)
                                            .vidType(vidType)
                                            .wid(wid)
                                            .build();
                                    List<PcMenuTreeDTO> treeDTOList = productFacade
                                            .getPrivilegeTreeByInstanceId(queryPrivilegeTreeDto);
                                    KeyOrMenuResVo keyOrMenuResVo = new KeyOrMenuResVo();
                                    keyOrMenuResVo.setCodeList(privilegeCodes);
                                    keyOrMenuResVo.setMenus(treeDTOList);
                                    keyOrMenuResVo.setProductId(proId);
                                    return keyOrMenuResVo;
                                });
                        completableFutures.add(asyncCompletable);
                    }
                    CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()])).join();
                    List<KeyOrMenuResVo> keyOrMenuResVos = new ArrayList<>();
                    for (CompletableFuture<KeyOrMenuResVo> completableFuture : completableFutures) {
                        try {
                            keyOrMenuResVos.add(completableFuture.get());
                        } catch (Exception e) {
                            log.error("keyOrMenu CompletableFuture get error:", e);
                            throw new IllegalStateException("keyOrMenu CompletableFuture get() error");
                        }
                    }
                    Map<Long, KeyOrMenuResVo> productId2Res = keyOrMenuResVos.stream()
                            .collect(Collectors.toMap(KeyOrMenuResVo::getProductId, Function.identity(), (key1,
                                                                                                          key2) -> key2));
                    KeyOrMenuResVo keyOrMenuResVo = new KeyOrMenuResVo();
                    keyOrMenuResVo.setCodeList(new ArrayList<>());
                    keyOrMenuResVo.setMenus(new ArrayList<>());
                    for (Long id : productIds) {
                        KeyOrMenuResVo menuResVo = productId2Res.get(id);
                        if (Objects.nonNull(menuResVo) && CollectionUtils.isNotEmpty(menuResVo.getCodeList())) {
                            keyOrMenuResVo.getCodeList().addAll(menuResVo.getCodeList());
                        }
                        if (Objects.nonNull(menuResVo) && CollectionUtils.isNotEmpty(menuResVo.getMenus())) {
                            keyOrMenuResVo.getMenus().addAll(menuResVo.getMenus());
                        }
                    }
                    return SoaTransformUtil.success(keyOrMenuResVo);
                }
            }
        }


        codeList = getPrivilegeCodes(productInstanceId, currentNode, bosId, wid, vidType, blendNode);
        QueryPrivilegeTreeDto queryPrivilegeTreeDto = QueryPrivilegeTreeDto.builder()
                .appChecked(false)
                .bosId(bosId)
                .channelGroup(null)
                .channelScene(SceneItemValueEnum.CHANNEL_PC)
                .codeList(codeList)
                .instance(flowDTO)
                .markOrFilter(false)
                .menuTypeEnum(MenuTypeEnum.PC)
                .roleTypeEnum(null)
                .showModelEnum(null)
                .vid(currentNode)
                .vidType(vidType)
                .wid(wid)
                .build();
        List<PcMenuTreeDTO> pcMenuTreeDTOS =
                productFacade.getPrivilegeTreeByInstanceId(queryPrivilegeTreeDto);
        //组装返回值
        KeyOrMenuResVo keyOrMenuResVo = new KeyOrMenuResVo(pcMenuTreeDTOS, codeList, null);
        return SoaTransformUtil.success(keyOrMenuResVo);
    }

    @Override
    public List<String> getPrivilegeCodes(Long productInstanceId, Long currentNode, Long bosId,
                                          Long wid, Integer vidType, boolean blendNode) {
        //是融合节点
        if (blendNode) {
            //是根节点
            if (!Constants.DEFAULT_VID.equals(currentNode)) {
                if (privilegeFacade.instanceIdShowOrNot(wid, bosId, DEFAULT_VID, DEFAULT_VID_TYPE, productInstanceId)) {
                    return privilegeFacade
                            .queryCodeListByVids(wid, bosId, SceneItemValueEnum.CHANNEL_PC.getItemValue(),
                                    productInstanceId, new RootNode(currentNode, vidType, productInstanceId),
                                    new RootNode(Constants.DEFAULT_VID, Constants.DEFAULT_VID_TYPE, productInstanceId));
                }
            } else {
                //currentNode:店铺节点
                RootNode rootNode = structureFacade.getRootNode(bosId);
                if (Objects.nonNull(rootNode)) {
                    if (privilegeFacade.instanceIdShowOrNot(wid, bosId, rootNode.getVid(), rootNode.getVidType(), productInstanceId)) {
                        rootNode.setInstanceId(productInstanceId);
                        return privilegeFacade.queryCodeListByVids(wid, bosId, SceneItemValueEnum.CHANNEL_PC.getItemValue(),
                                productInstanceId, rootNode,
                                new RootNode(Constants.DEFAULT_VID, Constants.DEFAULT_VID_TYPE, productInstanceId));
                    }
                }
            }
        }
        return privilegeFacade.queryCodeList(wid, bosId, currentNode, vidType, SceneItemValueEnum.CHANNEL_PC.getItemValue(),
                productInstanceId);
    }

    @Override
    public CrossRegionResVo getIsCrossRegion(CrossRegionReqVo reqVo) {
        CrossRegionResVo resVo = new CrossRegionResVo();

        com.weimob.mp.merchant.authbase.ability.model.req.subject.QuerySubjectRoleRelationRequest request = com
                .weimob.mp.merchant.authbase.ability.model.req.subject.QuerySubjectRoleRelationRequest.builder()
                .bosId(reqVo.getBosId())
                .bizId(reqVo.getWid())
                .bizType(BizType.ACCOUNT.ordinal())
                .build();
        SoaResponse<SubjectRoleRelationResponse, Void> soaResponse = subjectRoleAuthService.getSubjectRoleRelation(request);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        Boolean isSuperRole = soaResponse.getResponseVo().getSubjectVidRoleDTOS().stream()
                .anyMatch(role -> PublicIsOrNotEnum.FALSE.getIntCode().equals(role.getStatus()) && Constants.DEFAULT_VID
                        .equals(role.getVid()) && role.getRoleType().equals(RoleTypeEnum.SYSTEM_DEFAULT_SUPER_ROLE.getRoleType()));
        //跨区域权限：非超管 增购跨区域能力附属功能 有多个区域权限
        if (isSuperRole) {
            return resVo;
        }
        Long defaultProductInstanceId = productFacade.getDefaultProductInstanceId(reqVo.getBosId());
        AttachedFunctionQuery query = new AttachedFunctionQuery(new ProductInstanceId(defaultProductInstanceId));
        query.setKeys(Arrays.asList(Constants.CROSS_DISTRICT));
        SoaResponse<AttachedFunctionDTO, Void> attachedSoaResponse = productInstanceContractQueryAbility.queryAttachedFunction(query);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(attachedSoaResponse), MpBizCode.build(attachedSoaResponse));
        Map<String, Object> functions = attachedSoaResponse.getResponseVo().getFunctions();
        if (MapUtils.isEmpty(functions) || Objects.isNull(functions.get(Constants.CROSS_DISTRICT))) {
            return resVo;
        }
        String crossDistrict = functions.get(Constants.CROSS_DISTRICT).toString();
        Object isCrossDistrict = JSON.parseArray(crossDistrict).get(0);
        if (!BooleanUtil.toBoolean(isCrossDistrict.toString())) {
            return resVo;
        }
        QueryVidInfoByVidTypeResponse queryVidInfo = privilegeFacade.querySelectableVidsByVidType(reqVo
                .getBosId(), reqVo.getWid(), VidTypeEnum.DISTRICT.getType(), null, null, Constants.FIVE_PAGE_SIZE);
        if (CollectionUtils.isNotEmpty(queryVidInfo.getList()) && queryVidInfo.getList().size() > 1) {
            resVo.setIsCrossRegion(Boolean.TRUE);
            return resVo;
        }
        return resVo;
    }

    @Override
    public HttpResponse<VidListByAuthCodeResVo> getVidListByAuthCode(VidListByAuthCodeReqVo reqVo) {
        QueryVidByCodeAndProductInstanceIdRequest request = QueryVidByCodeAndProductInstanceIdRequest.builder()
                .lastId(reqVo.getLastId())
                .pageSize(reqVo.getPageSize())
                .wid(reqVo.getWid())
                .bosId(reqVo.getBosId())
                .vidType(reqVo.getSelectVidType())
                .productInstanceId(reqVo.getProductInstanceId())
                .code(reqVo.getAuthCode())
                .build();
        SoaResponse<QueryVidInfoByProdctInstanceIdAndCodeResponse, Void> soaResponse = integrationExportService.queryVidByCodeAndProductInstance(request);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        QueryVidInfoByProdctInstanceIdAndCodeResponse responseVo = soaResponse.getResponseVo();
        VidListByAuthCodeResVo resVo = new VidListByAuthCodeResVo();
        resVo.setLastId(responseVo.getLastId());
        resVo.setMore(responseVo.getMore());
        //查询是否有子节点
        List<Long> vids = responseVo.getList().stream().map(VidInfoDTO::getVid).distinct().collect(toList());
        Map<Long, Boolean> childMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(vids)) {
            QueryVidPageReqDTO reqDTO = new QueryVidPageReqDTO();
            reqDTO.setPage(Constants.DEFAULT_FIRST_PAGE_NUM);
            reqDTO.setSize(Constants.MAX_PAGE_SIZE);
            reqDTO.setBosId(reqVo.getBosId());
            reqDTO.setVidList(vids);
            List<String> resultData = new ArrayList<>();
            resultData.add(NodeSearchParentNodeEnum.HAS_CHILD.getCode());
            reqDTO.setResultData(resultData);
            SoaResponse<VidPageRespDTO, Void> childSoaResponse = nodeSearchExportService.queryVidPage2(reqDTO);
            ExceptionUtils.checkState(SoaRespUtils.isSuccess(childSoaResponse), MpBizCode.build(childSoaResponse));
            childMap.putAll(childSoaResponse.getResponseVo().getVidInfos().stream().collect(toMap(VidPageInfoDTO::getVid, VidPageInfoDTO::isHasChildren)));
        }
        List<VidByAuthCodeDto> vidInfoList = new ArrayList<>();
        EnhancedBeanCopyUtils.copy(responseVo.getList(), vidInfoList, VidByAuthCodeDto.class, (o1, o2) -> {
            o2.setVidType(reqVo.getSelectVidType());
            if (Objects.nonNull(childMap.get(o1.getVid())) && childMap.get(o1.getVid())) {
                o2.setHasChildren(Boolean.TRUE);
            }
        });
        resVo.setVidInfoList(vidInfoList);
        return SoaTransformUtil.success(resVo);
    }

    @Override
    public HttpResponse<VidByPrentVidsResVo<AttributionNodeDto>> selectVidByPrentVids(VidByPrentVidsReqVo reqVo) {
        if (reqVo.getPageSize() > Constants.MAX_PAGE_SIZE) {
            reqVo.setPageSize(Constants.MAX_PAGE_SIZE);
        }
        QueryVidPageReqDTO reqDTO = new QueryVidPageReqDTO();
        reqDTO.setPage(reqVo.getPageNum());
        reqDTO.setSize(reqVo.getPageSize());
        reqDTO.setBosId(reqVo.getBosId());
        reqDTO.setVidTypes(reqVo.getVidTypeList());
        reqDTO.setDepthLayer(Boolean.TRUE);
        reqDTO.setVidName(reqVo.getVidName());
        reqDTO.setProductInstanceIdList(reqVo.getProductInstanceIdList());
        reqDTO.setParentVidList(reqVo.getParentVidList());
        reqDTO.setVidStatus(reqVo.getVidStatus());
        reqDTO.setVidList(reqVo.getVidList());
        SoaResponse<VidPageRespDTO, Void> soaResponse = nodeSearchExportService.queryVidPage2(reqDTO);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        List<VidPageInfoDTO> vidInfoList = soaResponse.getResponseVo().getVidInfos();
        List<AttributionNodeDto> nodeDtos = new ArrayList<>();
        List<Long> queryVidList = vidInfoList.stream()
                .map(VidPageInfoDTO::getParentVid).filter(o -> !DEFAULT_VID.equals(o))
                .distinct().collect(Collectors.toList());
        List<Long> selectVids = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(reqVo.getSelectVids())) {
            selectVids = reqVo.getSelectVids().stream().filter(Objects::nonNull).collect(toList());
            if (CollectionUtils.isNotEmpty(selectVids)) {
                queryVidList.addAll(selectVids);
            }
        }
        Map<Long, NodeBaseInfoDto> vidNameMap = structureFacade.selectVidName(queryVidList);
        EnhancedBeanCopyUtils.copy(vidInfoList, nodeDtos, AttributionNodeDto.class, (o1, o2) -> {
            o2.setVidType(o1.getVidTypes().get(0));
            o2.setParentVidName(Objects.nonNull(vidNameMap.get(o1.getParentVid())) ? vidNameMap.get(o1.getParentVid()).getVidName() : null);
            o2.setVidName(o1.getBaseInfo().getVidName());
            o2.setHasChildren(o1.isHasChildren());
            o2.setVidStatus(o1.getBaseInfo().getVidStatus());
            if (CollectionUtils.isNotEmpty(o1.getExtInfos())) {
                String wxOnlineStatus = o1.getExtInfos().get(0).getExtInfoMap().get(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey());
                if (StringUtils.isNotBlank(wxOnlineStatus)) {
                    o2.setWxOnlineStatus(Integer.parseInt(wxOnlineStatus));
                }
            }
        });
        VidByPrentVidsResVo<AttributionNodeDto> resVo = new VidByPrentVidsResVo<>();
        resVo.setPageNumber(reqVo.getPageNum());
        resVo.setPageSize(reqVo.getPageSize());
        resVo.setMore(soaResponse.getResponseVo().getMore());
        resVo.setVidInfoList(nodeDtos);
        if (CollectionUtils.isNotEmpty(selectVids)) {
            List<SelectVidInfo> selectVidInfos = new ArrayList<>();
            for (Long selectVid : selectVids) {
                SelectVidInfo vidInfo = new SelectVidInfo();
                vidInfo.setSelectVid(selectVid);
                vidInfo.setSelectVidName(Objects.nonNull(vidNameMap.get(selectVid)) ? vidNameMap.get(selectVid).getVidName() : null);
                selectVidInfos.add(vidInfo);
            }
            resVo.setSelectVidInfoList(selectVidInfos);
        }
        return SoaTransformUtil.success(resVo);
    }

    @Override
    public HttpResponse<PageScrollDto<GroupInfoDto>> selectGroupInfoList(SelectGroupInfoListReqVo reqVo) {
        FindAllGroupIdByBosIdAndNameReqDTO reqDTO = new FindAllGroupIdByBosIdAndNameReqDTO();
        if (reqVo.getParentVid() != null) {
            RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
            if (rootNode == null) {
                return SoaTransformUtil.build(MpBizCode.SETTLE_CHANNEL_ROOT_NODE_CHECK_ERROR);
            }
            if (reqVo.getParentVid().equals(rootNode.getVid())
                    || reqVo.getParentVid().equals(ROOT_PARENT_VID)) {
                reqDTO.setParentVid(null);
            } else {
                reqDTO.setParentVid(reqVo.getParentVid());
                reqDTO.setContainParentVid(Boolean.TRUE);
            }
        }
        reqDTO.setBosId(reqVo.getBosId());
        reqDTO.setGroupName(reqVo.getGroupName());
        reqDTO.setGroupType(reqVo.getGroupType());
        reqDTO.setSize(reqVo.getPageSize());
        reqDTO.setScrollId(Strings.isNotBlank(reqVo.getScrollId()) ? reqVo.getScrollId() : "0");
        FindAllGroupIdByBosIdAndNameRespDTO
                respDTO = structureFacade.findAllGroupIdByBosIdAndName(reqDTO);
        PageScrollDto<GroupInfoDto> pageScrollDto = new PageScrollDto<>();
        if (respDTO != null && CollectionUtils.isNotEmpty(respDTO.getGroupInfoList())) {
            pageScrollDto.setMore(respDTO.getAndMore());
            pageScrollDto.setScrollId(respDTO.getLastScrollId());
            pageScrollDto.setDataList(convertGroupInfoDto(respDTO.getGroupInfoList()));
        } else {
            pageScrollDto.setMore(Boolean.FALSE);
            pageScrollDto.setDataList(new ArrayList<>());
        }
        return SoaTransformUtil.success(pageScrollDto);
    }

    private List<GroupInfoDto> convertGroupInfoDto(List<BusinessGroupInfoDTO> groupInfoList) {
        List<GroupInfoDto> groupInfoDtoList = new ArrayList<>(groupInfoList.size());
        groupInfoList.forEach(dto -> {
            GroupInfoDto groupInfoDto = new GroupInfoDto();
            groupInfoDto.setGroupId(dto.getGroupId());
            groupInfoDto.setGroupName(dto.getGroupName());
            groupInfoDto.setGroupType(dto.getGroupType());
            groupInfoDtoList.add(groupInfoDto);
        });
        return groupInfoDtoList;
    }

}
