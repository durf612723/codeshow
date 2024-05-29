package com.weimob.mp.merchant.backstage.server.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.excel.annotation.ExcelProperty;
import com.alibaba.excel.exception.ExcelAnalysisException;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weimob.dep.crm.rpc.dubbo.common.utils.SoaResponseUtils;
import com.weimob.menglian.spi.common.annotation.MenglianSpiReference;
import com.weimob.mp.merchant.ability.ProductInstanceContractQueryAbility;
import com.weimob.mp.merchant.ability.dto.AttachedFunctionDTO;
import com.weimob.mp.merchant.ability.request.query.AttachedFunctionBatchQuery;
import com.weimob.mp.merchant.api.dto.node.AddressDTO;
import com.weimob.mp.merchant.api.dto.node.BusinessTimeDTO;
import com.weimob.mp.merchant.api.dto.node.VidBaseInfoDTO;
import com.weimob.mp.merchant.api.dto.node.VidExtInfosDTO;
import com.weimob.mp.merchant.api.dto.response.CreateVidVo;
import com.weimob.mp.merchant.api.product.ProductFlowCommandApi;
import com.weimob.mp.merchant.api.product.ProductFlowQueryApi;
import com.weimob.mp.merchant.api.product.dto.ProductInstanceFlowDTO;
import com.weimob.mp.merchant.api.product.dto.ProductInstanceForActivationDTO;
import com.weimob.mp.merchant.api.product.request.command.OneVidActivateMultipleInstancesFlowCommand;
import com.weimob.mp.merchant.api.product.request.query.ProductInstanceListForImportVidQuery;
import com.weimob.mp.merchant.backstage.ability.dto.req.ExcelResult;
import com.weimob.mp.merchant.backstage.ability.dto.req.enums.FileStatusEnum;
import com.weimob.mp.merchant.backstage.api.domain.dto.RoleInfoDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.RootNode;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.product.QueryAllProductInstanceReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.product.QueryInstanceListForActiveReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.share.ImporSelectNodeReqVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.request.structure.*;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.product.ActiveProductInstanceDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.product.InstanceForActiveResVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.product.MainProductInstanceDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.product.ProductDto;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.structure.NodeListVo;
import com.weimob.mp.merchant.backstage.api.domain.dto.response.structure.nodeprivilege.NodePrivilegeResVo;
import com.weimob.mp.merchant.backstage.api.domain.enums.FloorNameEnum;
import com.weimob.mp.merchant.backstage.common.utils.*;
import com.weimob.mp.merchant.backstage.server.constant.BackstageErrorCode;
import com.weimob.mp.merchant.backstage.server.constant.Constants;
import com.weimob.mp.merchant.backstage.server.constant.MainContentProperties;
import com.weimob.mp.merchant.backstage.server.constant.MpBizCode;
import com.weimob.mp.merchant.backstage.server.dto.*;
import com.weimob.mp.merchant.backstage.server.dto.exportnode.*;
import com.weimob.mp.merchant.backstage.server.dto.exportnode.ExportNodeInfo;
import com.weimob.mp.merchant.backstage.server.dto.importnode.*;
import com.weimob.mp.merchant.backstage.server.enums.*;
import com.weimob.mp.merchant.backstage.server.facade.ProductFacade;
import com.weimob.mp.merchant.backstage.server.facade.StructureFacade;
import com.weimob.mp.merchant.backstage.server.service.*;
import com.weimob.mp.merchant.backstage.server.service.spi.ExtFieldSpiService;
import com.weimob.mp.merchant.backstage.server.tool.CheckImportExtFieldsResult;
import com.weimob.mp.merchant.backstage.server.tool.VidExtFieldExportTool;
import com.weimob.mp.merchant.backstage.server.tool.VidExtFieldImportTool;
import com.weimob.mp.merchant.backstage.server.util.*;
import com.weimob.mp.merchant.backstage.upload.DownloadCenterUploadService;
import com.weimob.mp.merchant.backstage.upload.model.UploadFileDTO;
import com.weimob.mp.merchant.common.enums.PublicAuxiliaryFunctionEnum;
import com.weimob.mp.merchant.common.enums.VidTypeEnum;
import com.weimob.mp.merchant.common.exception.MerchantErrorCode;
import com.weimob.mp.merchant.common.ext.ContactTelDTO;
import com.weimob.mp.merchant.common.util.BeanCopyUtils;
import com.weimob.mp.merchant.common.util.FunctionUtil;
import com.weimob.mp.merchant.common.util.exception.MerchantBaseNoStackTraceException;
import com.weimob.mp.merchant.product.common.BatchOperateResult;
import com.weimob.mp.merchant.product.common.OperateResult;
import com.weimob.mp.merchant.product.enums.ActivationStatusEnum;
import com.weimob.mp.merchant.product.types.BosId;
import com.weimob.mp.merchant.product.types.ProductInstanceId;
import com.weimob.mp.merchant.product.types.Vid;
import com.weimob.mp.merchant.product.types.VidType;
import com.weimob.mp.merchant.productcenter.ability.api.ProductBaseInfoAbility;
import com.weimob.mp.merchant.productcenter.ability.api.ProductNodeExtConfigAbility;
import com.weimob.mp.merchant.productcenter.ability.api.WhiteInfoAbility;
import com.weimob.mp.merchant.productcenter.ability.dto.*;
import com.weimob.mp.merchant.productcenter.ability.request.ProductNodeExtImportConfigQuery;
import com.weimob.mp.merchant.productcenter.ability.request.QueryProductBaseInfoByIdsRequest;
import com.weimob.mp.merchant.productcenter.ability.request.QueryWhiteInfoPageRequest;
import com.weimob.mp.merchant.productcenter.common.enums.whiteinfo.WhiteTypeEnum;
import com.weimob.mp.merchant.spi.MerchantGetUsableExtFieldKeysSpi;
import com.weimob.mp.merchant.spi.dto.MerchantGetUsableExtFieldKeysSpiReqDTO;
import com.weimob.mp.merchant.spi.dto.resp.MerchantGetUsableExtFieldKeysSpiRespDTO;
import com.weimob.mp.merchantstructure.ability.BosExportService;
import com.weimob.mp.merchantstructure.ability.NodeInfoExportService;
import com.weimob.mp.merchantstructure.ability.NodeSearchExportService;
import com.weimob.mp.merchantstructure.ability.NodeTagExportService;
import com.weimob.mp.merchantstructure.ability.domain.request.QueryVidExtInfoDto;
import com.weimob.mp.merchantstructure.ability.domain.request.QueryBosReqDto;
import com.weimob.mp.merchantstructure.ability.domain.request.QueryVidExtInfoDto;
import com.weimob.mp.merchantstructure.ability.domain.request.bos.GetVidTypeByBosReqDTO;
import com.weimob.mp.merchantstructure.ability.domain.request.search.QueryVidPageReqDTO;
import com.weimob.mp.merchantstructure.ability.domain.request.search.VidPageExtInfoReqDTO;
import com.weimob.mp.merchantstructure.ability.domain.request.search.VidPageExtMatchReqDTO;
import com.weimob.mp.merchantstructure.ability.domain.request.search.VidPageOrderReqDTO;
import com.weimob.mp.merchantstructure.ability.domain.request.tag.FindBosTagByNameReqDTO;
import com.weimob.mp.merchantstructure.ability.domain.response.bos.GetVidTypeByBosRespDTO;
import com.weimob.mp.merchantstructure.ability.domain.response.node.GetVidListByCodeRespDTO;
import com.weimob.mp.merchantstructure.ability.domain.response.search.VidPageRespDTO;
import com.weimob.mp.merchantstructure.ability.domain.response.tag.FindBosTagByNameRespDTO;
import com.weimob.mp.merchantstructure.common.domain.common.BusinessOsDto;
import com.weimob.mp.merchantstructure.common.domain.common.ExtInfoDTO;
import com.weimob.mp.merchantstructure.common.domain.common.BusinessOsDto;
import com.weimob.mp.merchantstructure.common.domain.common.NodeExtInfoDto;
import com.weimob.mp.merchantstructure.common.domain.common.VidInfoDto;
import com.weimob.mp.merchantstructure.common.domain.common.search.VidPageInfoDTO;
import com.weimob.mp.merchantstructure.common.domain.common.tag.NodeTagDTO;
import com.weimob.mp.merchantstructure.common.domain.tag.BosTagDTO;
import com.weimob.mp.merchantstructure.common.enums.*;
import com.weimob.mp.passport.account.ability.response.PhoneZoneInfoResponse;
import com.weimob.saas.address.enums.MapSystemEnum;
import com.weimob.saas.address.enums.SourceEnum;
import com.weimob.saas.address.model.request.division.DivisionRequest;
import com.weimob.saas.address.model.response.AddressCommonError;
import com.weimob.saas.address.model.response.division.ParsedDivisionBO;
import com.weimob.saas.address.service.AdministrativeDivisionService;
import com.weimob.saas.common.spf.core.lib.context.GlobalErr;
import com.weimob.saas.common.spf.core.lib.utils.GEPreconditions;
import com.weimob.saas.common.spf.core.lib.utils.SoaRespUtils;
import com.weimob.saas.mall.common.response.HttpResponse;
import com.weimob.soa.common.response.SoaResponse;
import com.weimob.starlinker.common.exceptions.BizException;
import javassist.*;
import javassist.Modifier;
import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ConstPool;
import javassist.bytecode.annotation.Annotation;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.sf.cglib.beans.BeanMap;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.util.Strings;
import org.apache.poi.ss.formula.functions.T;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.weimob.mp.merchant.backstage.server.constant.Constants.*;
import static com.weimob.mp.merchant.backstage.server.enums.NodeImportError.*;
import static java.util.stream.Collectors.*;

@Service
@Slf4j
public class NodeImportServiceImpl implements NodeImportService {

    @Reference
    private ProductNodeExtConfigAbility nodeExtConfigAbility;
    @Reference
    private WhiteInfoAbility whiteInfoAbility;
    @Reference
    private ProductFlowQueryApi productFlowApi;
    @Reference
    private ProductBaseInfoAbility productBaseInfoAbility;
    @Autowired
    private StructureFacade structureFacade;
    @Reference
    private AdministrativeDivisionService administrativeDivisionService;
    @Reference
    private NodeTagExportService nodeTagExportService;
    @Autowired
    private NodePrivilegeService nodePrivilegeService;
    @Autowired
    private StructureService structureService;
    @Autowired
    private ApiTemplate apiTemplate;
    @Reference
    private ProductFlowCommandApi productFlowCommandApi;
    @Reference
    private NodeInfoExportService nodeInfoExportService;
    @MenglianSpiReference
    private MerchantGetUsableExtFieldKeysSpi merchantGetUsableExtFieldKeysSpi;
    @Reference
    private NodeSearchExportService nodeSearchExportService;
    @Autowired
    private ProductFacade productFacade;
    @Reference
    private BosExportService bosExportService;
    @Autowired
    private MainContentProperties mainContentProperties;
    @Autowired
    private VidExtFieldService vidExtFieldService;
    @Autowired
    private DownloadCenterUploadService downloadCenterUploadService;
    @Resource
    private ExtFieldSpiService extFieldSpiService;
    @Reference
    private ProductInstanceContractQueryAbility productInstanceQueryAbility;
    @Autowired
    private ProductService productService;

    private static final String WHITE_BOS_HEADER = "是否开通##应用";

    private static final String NODE_EXT_NAME = "##设置";

    private static final String PRODUCTNAME = "productName";

    private String ACTIVE_MODE_ANNOTATION = "组织开通%s会自动开通";

    @Value("${import.max.size:20000}")
    private Integer importMaxSize;
    @Value("${export.max.size:20000}")
    private Integer exportMaxSize;

    public static final HashSet<String> addressKeySet = new HashSet<String>() {
        {
            add(ExtFieldKeyEnum.ADDRESS_PROVINCE_NAME.getKey());//省
            add(ExtFieldKeyEnum.ADDRESS_CITY_NAME.getKey());//市
            add(ExtFieldKeyEnum.ADDRESS_COUNTY_NAME.getKey());//区
            add(ExtFieldKeyEnum.ADDRESS_DETAIL.getKey());//详细地址
        }
    };

    /**
     * 模板生成
     *
     * @param reqVo              请求参数
     * @param isImport           是否导入操作/是否需要错误报告
     * @param isImportTemplate   是否导入模板
     * @param isCheckActiveModel 是否季铵盐模版应用
     * @return
     */
    @Override
    public Class templateExport(NodeFileReqDto reqVo, Boolean isImport, Boolean isImportTemplate, Boolean isCheckActiveModel) {
        Long bosId = reqVo.getBosId();
        Long vid = reqVo.getVid();
        //字段key-name映射 ExcelProperty注解使用
        Map<String, List<String>> keyNameMap = Maps.newLinkedHashMap();
        //字段key-writeRule映射 ExcelAnnotation注解使用
        Map<String, String> excelAnnotationMap = Maps.newLinkedHashMap();
        List<String> fields = new ArrayList<>();
        RootNode rootNode = structureFacade.getRootNode(bosId);
        String rootNodeName = "";
        if (Objects.nonNull(rootNode) && rootNode.getVidType().equals(reqVo.getSelectVidType())) {
            rootNodeName = mainContentProperties.getVidTypeName(bosId);
        }
        if (isCheckActiveModel && productFacade.queryActiveModel(bosId)) {
            //查询bosId下的使用中的应用
            List<ProductDto> productInfo = getProductInfo(bosId, reqVo.getSelectVidType());
            for (ProductDto productDto : productInfo) {
                String fieldKey = PRODUCTNAME + productDto.getProductId();
                fields.add(fieldKey);
                String productName = WHITE_BOS_HEADER.replaceAll("##", productDto.getProductName());
                keyNameMap.put(PRODUCTNAME + productDto.getProductId(), Arrays.asList(productName, productName));
                if (CollectionUtils.isNotEmpty(productDto.getFollowProductList())) {
                    String follow = productDto.getFollowProductList().stream()
                            .map(ProductDto::getProductName).collect(Collectors.joining("应用、")) + "应用";
                    String annotation = String.format(ACTIVE_MODE_ANNOTATION, productDto.getProductName());
                    excelAnnotationMap.put(fieldKey, annotation + follow);
                }
            }
        }
        //获取所有应用（包含过期的）对应扩展字段
        List<ProductInstanceFlowDTO> activeProductList = new ArrayList<>();
        List<ProductInstanceFlowDTO> productInstanceFlowDTOS = checkExpandFiledForBos(bosId, reqVo.getSelectVidType());
        activeProductList.addAll(productInstanceFlowDTOS);
        List<Long> productIds = productInstanceFlowDTOS.stream().map(ProductInstanceFlowDTO::getProductId).collect(toList());
        List<ProductNodeExtImportConfigDTO> importConfigDTOS = expandFiled(productIds, reqVo.getSelectVidType());
        Map<Long, List<ProductNodeExtImportConfigDTO>> productExtMap = importConfigDTOS.stream()
                .collect(Collectors.groupingBy(ProductNodeExtImportConfigDTO::getProductId));
        if (productExtMap != null && productExtMap.size() > 0) {
            for (ProductInstanceFlowDTO productDto : activeProductList) {
                if (Strings.isBlank(productDto.getProductName())) {
                    continue;
                }
                String firstHeader = NODE_EXT_NAME.replaceAll("##", productDto.getProductName());
                List<ProductNodeExtImportConfigDTO> configDTOS = productExtMap.get(productDto.getProductId());
                if (CollectionUtils.isNotEmpty(configDTOS)) {
                    // 按域拆分
                    Map<String/* businessDomain */, List<ProductNodeExtImportConfigDTO>> businessDomainConfigsMap =
                            FunctionUtil.selectListMap(configDTOS, config -> config.getFieldDomain() != null ? config.getFieldDomain() : "");

                    businessDomainConfigsMap.forEach((businessDomain, configs) -> {
                        Long productId = productDto.getProductId();
                        Long productInstanceId = productDto.getProductInstanceId();
                        List<String> extFieldKeyList = FunctionUtil.select(configs, ProductNodeExtImportConfigDTO::getFieldKey);
                        List<String> extFieldKeyBySpi = getExtFieldKeyBySpi(bosId, productId, productInstanceId, extFieldKeyList, businessDomain);
                        for (ProductNodeExtImportConfigDTO configDTO : configDTOS) {
                            if (extFieldKeyBySpi.contains(configDTO.getFieldKey())) {
                                String fieldKey = configDTO.getFieldKey() + productDto.getProductId();
                                fields.add(fieldKey);
                                String fieldName = configDTO.getFieldName();
                                if (Objects.equals(configDTO.getFieldRequire(), 0)) {
                                    fieldName = fieldName + "*";
                                }
                                keyNameMap.put(fieldKey, Arrays.asList(firstHeader, fieldName));
                                if (StringUtils.isNotBlank(configDTO.getWriteRule())) {
                                    excelAnnotationMap.put(fieldKey, configDTO.getWriteRule());
                                }
                            }
                        }
                    });
                }
            }
        }
        if (isImport) {
            keyNameMap.put("error", Arrays.asList("错误原因", "错误原因"));
        }
        try {
            Class templateClass = getTemplateClass(reqVo.getSelectVidType(), isImportTemplate);
            Object exportNode = generateObject(fields, templateClass, isImport, excelAnnotationMap.keySet());
            //根节点则将基础表头中组织类型名称替换为根节点名称
            Field[] declaredFields = exportNode.getClass().getSuperclass().getDeclaredFields();
            for (Field field : declaredFields) {
                if (field.isSynthetic()) {
                    continue;
                }
                if (StringUtils.isNotBlank(rootNodeName)) {
                    ExcelProperty annotation = field.getAnnotation(ExcelProperty.class);
                    InvocationHandler handler = Proxy.getInvocationHandler(annotation);
                    Field memberValues = handler.getClass().getDeclaredField("memberValues");
                    memberValues.setAccessible(true);
                    Map valueMap = (Map) memberValues.get(handler);
                    String[] value = (String[]) valueMap.get("value");
                    List<String> valueList = Lists.newArrayList(value);
                    valueList.removeAll(Collections.singleton(""));
                    String vidTypeName = VidTypeEnum.enumByCode(reqVo.getSelectVidType()).getDesc();
                    final String rooName = rootNodeName;
                    List<String> newValues = valueList.stream().filter(val -> val.contains(vidTypeName))
                            .map(val -> val.replaceAll(vidTypeName, rooName)).collect(toList());
                    if (CollectionUtils.isNotEmpty(newValues)) {
                        valueMap.put("value", newValues.toArray(new String[newValues.size()]));
                    }

                }
            }
            for (Field field : exportNode.getClass().getDeclaredFields()) {
                String name = field.getName();
                if (!keyNameMap.keySet().contains(name)) {
                    continue;
                }
                if (field.isAnnotationPresent(ExcelProperty.class)) {
                    ExcelProperty annotation = field.getAnnotation(ExcelProperty.class);
                    InvocationHandler handler = Proxy.getInvocationHandler(annotation);
                    Field memberValues = handler.getClass().getDeclaredField("memberValues");
                    memberValues.setAccessible(true);
                    Map valueMap = (Map) memberValues.get(handler);

                    String[] value = (String[]) valueMap.get("value");
                    List<String> valueList = Lists.newArrayList(value);
                    valueList.removeAll(Collections.singleton(""));
                    if (CollectionUtils.isEmpty(valueList)) {
                        List<String> headers = keyNameMap.get(field.getName());
                        valueMap.put("value", headers.toArray(new String[headers.size()]));
                    }
                }
                if (excelAnnotationMap.keySet().contains(name) && field.isAnnotationPresent(ExcelAnnotation.class)) {
                    ExcelAnnotation annotation = field.getAnnotation(ExcelAnnotation.class);
                    InvocationHandler handler = Proxy.getInvocationHandler(annotation);
                    Field memberValues = handler.getClass().getDeclaredField("memberValues");
                    memberValues.setAccessible(true);
                    Map valueMap = (Map) memberValues.get(handler);
                    String value = (String) valueMap.get("value");
                    if (StringUtils.isBlank(value)) {
                        String excelAnn = excelAnnotationMap.get(name);
                        valueMap.put("value", excelAnn);
                    }
                }
            }
            return exportNode.getClass();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("模板生成异常：{}", e.getStackTrace());
            ExceptionUtils.checkState(false, "模板生成失败");
            return null;
        }
    }

    @Override
    public void asyncImportAndReturnFailFile(ImportNodeReqVo reqVo, UploadFileDTO uploadFileDTO, Long downloadCenterId) {
        CompletableFuture.runAsync(() -> {
            List<Object> nodeInfoList = null;
            Class<Object> clazz = templateExport(reqVo, true, true,true);
            try {
                URL url = new URL(reqVo.getFileUrl());
                try (InputStream in = url.openStream()) {
                    nodeInfoList = ExcelParser.getInstance().fromSource(in).parse(clazz, 2);
                }
            } catch (IOException e) {
                ExceptionUtils.checkState(false, MpBizCode.IMPORT_EXCEL_ERROR);
            } catch (ExcelAnalysisException e) {
                ExceptionUtils.checkState(false, new MpBizCode(e.getMessage()));
            }
            ImportResDto<Object> resDto = importNodes(nodeInfoList, reqVo);
            File errorFile = null;
            Integer importTotalCount = nodeInfoList.size();
            if (CollectionUtils.isNotEmpty(resDto.getErrorList())) {
                errorFile = ExcelMaker.getInstance().makeFile(new File(uploadFileDTO.getFileName()), resDto.getErrorList(), clazz);
            } else {
                List<ImportAccountSuccessInfo> successInfoList = Collections.singletonList(new ImportAccountSuccessInfo(
                        String.format(Constants.IMPORT_RESULT, importTotalCount, importTotalCount)));
                errorFile = ExcelMaker.getInstance().makeFile(new File(NODE_IMPORT_FILENAME + EXCEL_TYPE), successInfoList, ImportAccountSuccessInfo.class);
                uploadFileDTO.setFileName(NODE_IMPORT_FILENAME + EXCEL_TYPE);
            }
            uploadFileDTO.setId(downloadCenterId);
            uploadFileDTO.setFile(errorFile);
            uploadFileDTO.setStatus(FileStatusEnum.SUCCESS.getType());
            ExcelResult excelResult = new ExcelResult();
            excelResult.setTotalCount(importTotalCount);
            excelResult.setFailCount(resDto.getErrorList().size());
            excelResult.setSuccessCount(importTotalCount - resDto.getErrorList().size());
            uploadFileDTO.setExcelResult(excelResult);
            downloadCenterUploadService.uploadFile(uploadFileDTO);
        }).exceptionally(e -> {
            uploadFileDTO.setId(downloadCenterId);
            uploadFileDTO.setStatus(FileStatusEnum.FAIL.getType());
            String errMsg;
            if (e.getCause() instanceof IllegalStateException) {
                errMsg = GlobalErr.getErrorCode().getUsererrmsg();
            } else {
                errMsg = Constants.IMPORT_ERROR;
            }
            uploadFileDTO.setUserMsg(errMsg);
            log.error("导入失败", e);
            downloadCenterUploadService.uploadFile(uploadFileDTO);
            return null;
        });
    }

    @Override
    public void asyncImportEditAndReturnFailFile(ImportNodeReqVo reqVo, UploadFileDTO uploadFileDTO, Long downloadCenterId) {
        CompletableFuture.runAsync(() -> {
            Long bosId = reqVo.getBosId();
            List<Integer> vidTypes = getBosVidTypes(bosId);
            //获取解析模板
            Map<String, Class> sheetClassMap = new HashMap<>();
            Map<Integer, Class> parseClass = new HashMap<>();
            RootNode rootNode = structureFacade.getRootNode(bosId);
            for (Integer vidType : vidTypes) {
                reqVo.setSelectVidType(vidType);
                //模版导出
                Class clazz = templateExport(reqVo, true, false, false);
                String sheetName = VidTypeEnum.enumByCode(vidType).getDesc();
                if (Objects.nonNull(rootNode) && rootNode.getVidType().equals(vidType)) {
                    sheetName = mainContentProperties.getVidTypeName(bosId);
                }
                sheetClassMap.put(sheetName, clazz);
                parseClass.put(vidType, clazz);
            }
            Map<String, List<Object>> nodeListMap = new HashMap<>();
            try {
                URL url = new URL(reqVo.getFileUrl());
                try (InputStream in = url.openStream()) {
                    nodeListMap = ExcelParser.getInstance().parseSheetList(in, sheetClassMap, 2);
                }
            } catch (IOException e) {
                ExceptionUtils.checkState(false, MpBizCode.IMPORT_EXCEL_ERROR);
            } catch (ExcelAnalysisException e) {
                ExceptionUtils.checkState(false, new MpBizCode(e.getMessage()));
            }
            List<Map<String, Object>> resultList = importEditNodes(reqVo, nodeListMap, parseClass, rootNode);
            File errorFile = null;
            Integer importTotalCount = nodeListMap.values().stream().mapToInt(List::size).sum();
            Integer failCount = resultList.stream().mapToInt(map -> ((List) map.get("data")).size()).sum();
            if (CollectionUtils.isNotEmpty(resultList)) {
                errorFile = ExcelMaker.getInstance().makeFile(new File(uploadFileDTO.getFileName()), resultList);
            } else {
                List<ImportAccountSuccessInfo> successInfoList = Collections.singletonList(new ImportAccountSuccessInfo(
                        String.format(Constants.IMPORT_RESULT, importTotalCount, importTotalCount)));
                errorFile = ExcelMaker.getInstance().makeFile(new File(NODE_IMPORT_EDIT_FILENAME + EXCEL_TYPE), successInfoList, ImportAccountSuccessInfo.class);
                uploadFileDTO.setFileName(NODE_IMPORT_EDIT_FILENAME + EXCEL_TYPE);
            }
            uploadFileDTO.setId(downloadCenterId);
            uploadFileDTO.setFile(errorFile);
            uploadFileDTO.setStatus(FileStatusEnum.SUCCESS.getType());
            ExcelResult excelResult = new ExcelResult();
            excelResult.setTotalCount(importTotalCount);
            excelResult.setFailCount(failCount);
            excelResult.setSuccessCount(importTotalCount - failCount);
            uploadFileDTO.setExcelResult(excelResult);
            downloadCenterUploadService.uploadFile(uploadFileDTO);
        }).exceptionally(e -> {
            uploadFileDTO.setId(downloadCenterId);
            uploadFileDTO.setStatus(FileStatusEnum.FAIL.getType());
            String errMsg;
            if (e.getCause() instanceof IllegalStateException) {
                errMsg = GlobalErr.getErrorCode().getUsererrmsg();
            } else {
                errMsg = Constants.IMPORT_ERROR;
            }
            uploadFileDTO.setUserMsg(errMsg);
            log.error("导入失败", e);
            downloadCenterUploadService.uploadFile(uploadFileDTO);
            return null;
        });
    }

    @Override
    public ImportResDto<Object> importNodes(List<Object> nodeInfoList, ImportNodeReqVo reqVo) {
        ImportResDto<Object> resDto = new ImportResDto<>();
        ExceptionUtils.checkState(nodeInfoList.size() <= importMaxSize, MpBizCode.IMPORT_MAX_ERROR);
        ExceptionUtils.checkState(org.apache.commons.collections.CollectionUtils.isNotEmpty(nodeInfoList), MpBizCode.IMPORT_IS_EMPTY);
        //失败列表
        List<Object> failList = new ArrayList<>();
        Long bosId = reqVo.getBosId();
        Integer vidType = reqVo.getSelectVidType();
        Long wid = reqVo.getWid();
        List<VidInfoDto> vidInfos = this.convertVidCodeAndType(reqVo.getBosId(),nodeInfoList);
        //查询需要开通应用
        List<ProductDto> productInfo = getProductInfo(bosId, reqVo.getSelectVidType());
        List<ProductDto> activeProductList = new ArrayList<>();
        activeProductList.addAll(productInfo);
        List<ProductDto> followActive = productInfo.stream().map(ProductDto::getFollowProductList)
                .flatMap(Collection::stream).filter(distinctByKey(ProductDto::getProductId)).collect(toList());
        activeProductList.addAll(followActive);

        activeProductList = activeProductList.stream().filter(o -> StringUtils.isNotBlank(o.getProductName())).collect(toList());
        List<ProductInstanceFlowDTO> productInstanceFlowDTOS = checkExpandFiledForBos(bosId, reqVo.getSelectVidType());
        List<Long> productIds = productInstanceFlowDTOS.stream().map(ProductInstanceFlowDTO::getProductId).collect(toList());
        List<ProductNodeExtImportConfigDTO> importConfigDTOS = expandFiled(productIds, reqVo.getSelectVidType());
        Map<Object, VidBaseInfoDTO> baseInfoMap = Maps.newHashMap();
        Map<Object, AddNodeReqVo> addNodeReqVoMap = Maps.newHashMap();
        Map<Object, List<ProductDto>> openProductListMap = Maps.newHashMap();
        Map<Object, List<ProductNodeExtImportConfigDTO>> importConfigMap = Maps.newHashMap();
        List<Object> waitAddNodeInfoList = Lists.newArrayList();
        Map<Long, Integer> vidTypeMap = Maps.newHashMap();

        Boolean activeModel = productFacade.queryActiveModel(bosId);

        importFor:
        for (Object nodeInfo : nodeInfoList) {
            Object info = validField(nodeInfo);
            if (Objects.nonNull(info)) {
                failList.add(nodeInfo);
                continue;
            }
            //校验上级组织id
            VidInfoDto vidInfoDto = FunctionUtil.find(vidInfos, v -> v.getVid().toString().equals(getKey(nodeInfo,
                    "parentVid")) && v.getBosId().equals(bosId));
            if (Objects.isNull(vidInfoDto)) {
                setValue(nodeInfo, "error", PARENTVID_NOT_EXIST.getMessage());
                failList.add(nodeInfo);
                continue;
            }
            //校验父组织节点是否支持此类型子组织
            if (!VidTypeEnum.enumByCode(vidType).getParentTypes().contains(vidInfoDto.getVidTypes().get(0))) {
                setValue(nodeInfo, "error", VIDTYPE_LESS.getMessage());
                failList.add(nodeInfo);
                continue;
            }
            if (StringUtils.isNotBlank(getKey(nodeInfo, "vidStatus"))
                    && VidStatusEnum.enumByDesc(getKey(nodeInfo, "vidStatus")) != null) {
                setValue(nodeInfo, "error", VIDSTATUS_REEOR.getMessage());
                failList.add(nodeInfo);
            }
            //校验是否有权限导入
            String parentPath = vidInfoDto.getPath();
            if (!Constants.DEFAULT_VID.equals(reqVo.getVid()) &&
                    !Arrays.asList(parentPath.split("-")).contains(reqVo.getVid().toString())) {
                setValue(nodeInfo, "error", PARENT_VIDTYPE_LESS.getMessage());
                failList.add(nodeInfo);
                continue;
            }

            vidTypeMap.putIfAbsent(vidInfoDto.getVid(), vidInfoDto.getVidTypes().get(0));

            //数据组装
            AddNodeReqVo addNodeReqVo = new AddNodeReqVo();
            addNodeReqVo.setBosId(bosId);
            addNodeReqVo.setMerchantId(reqVo.getMerchantId());
            addNodeReqVo.setWid(reqVo.getWid());
            addNodeReqVo.setVidType(vidType);
            addNodeReqVo.setParentVid(vidInfoDto.getVid());
            VidBaseInfoDTO baseInfo = new VidBaseInfoDTO();
            baseInfo.setVidName(getKey(nodeInfo, "vidName"));
            baseInfo.setVidCode(getKey(nodeInfo, "vidCode"));
            VidExtInfosDTO extInfo = new VidExtInfosDTO();
            Object checkResult = checkBaseExtInfo(nodeInfo, vidType, extInfo, baseInfo, null);
            if (Objects.nonNull(checkResult)) {
                failList.add(checkResult);
                continue;
            }
            //校验组织权限
            if (StringUtils.isNotBlank(getKey(nodeInfo, "vidPrivilege"))) {
                NodePrivilegeResVo nodePrivilege = nodePrivilegeService
                        .getNodePrivilege(bosId, reqVo.getVid(), vidType, getKey(nodeInfo, "vidPrivilege"));
                if (Objects.isNull(nodePrivilege)) {
                    setValue(nodeInfo, "error", String.format(NODE_PRIVILEGE_ERROR.getMessage(),getKey(nodeInfo, "vidPrivilege")));
                    failList.add(nodeInfo);
                    continue;
                }
                addNodeReqVo.setNodePrivilegeId(nodePrivilege.getRoleId());
            }
            //校验组织标签
            if (StringUtils.isNotBlank(getKey(nodeInfo, "vidTags"))) {
                List<Long> tagIds = checkNodeTagIds(bosId, nodeInfo);
                if (CollectionUtils.isEmpty(tagIds)) {
                    failList.add(nodeInfo);
                    continue;
                }
                addNodeReqVo.setNodeTagIds(tagIds);
            }
            baseInfo.setVidStatus(VidStatusEnum.ENABLE.getId());
            addNodeReqVo.setBaseInfo(baseInfo);
            addNodeReqVo.setExtInfo(extInfo);
            List<ProductDto> openProductList = new ArrayList<>();
            if (activeModel) {
                for (ProductDto productDto : productInfo) {
                    //校验产品字段
                    String productField = getKey(nodeInfo, PRODUCTNAME + productDto.getProductId());
                    if (StringUtils.isNotBlank(productField)) {
                        Boolean isOpen = PublicIsOrNotEnum.booleanForStr(productField);
                        if (Objects.isNull(isOpen)) {
                            setValue(nodeInfo, "error", WHITE_BOS_HEADER.replaceAll("##", productDto.getProductName()) +
                                    NodeImportError.DATA_ERROR.getMessage());
                            failList.add(nodeInfo);
                            continue importFor;
                        }

                        if (isOpen) {
                            openProductList.add(productDto);
                            openProductList.addAll(productDto.getFollowProductList());
                        }
                    }
                }
                if (CollectionUtils.isEmpty(openProductList)) {
                    setValue(nodeInfo,"error",NodeImportError.NOT_HAVE_EXTEND_PRODUCT.getMessage());
                    failList.add(nodeInfo);
                    continue importFor;
                }
            } else {
                openProductList.addAll(activeProductList);
            }
            // 临时保存
            baseInfoMap.put(nodeInfo, baseInfo);
            addNodeReqVoMap.put(nodeInfo, addNodeReqVo);
            openProductListMap.put(nodeInfo, openProductList);
            importConfigMap.put(nodeInfo, importConfigDTOS);
            waitAddNodeInfoList.add(nodeInfo);
        }

        // 扩展字段校验
        //      1. 填充扩展字段到tool
        //      2. 校验扩展字段
        VidExtFieldImportTool importTool = vidExtFieldService.instanceImportTool(bosId);
        for (Object nodeInfo : waitAddNodeInfoList) {
            List<ProductDto> openProductList = openProductListMap.get(nodeInfo);
            List<Long> productList = openProductList.stream().map(ProductDto::getProductId).collect(toList());
            appendExt(bosId, importTool, nodeInfo, importConfigDTOS, productList);
        }

        Map<Object, List<NodeExtInfoDto>> waitAddNodeExtInfos = Maps.newHashMap();
        List<Object> waitAddNodeInfoList2 = Lists.newArrayList();
        for (Object nodeInfo : waitAddNodeInfoList) {
            List<NodeExtInfoDto> extInfoList = new ArrayList<>();
            List<ProductDto> openProductList = openProductListMap.get(nodeInfo);
            // 扩展字段校验及封装
            Object node = validExt(bosId, importTool, nodeInfo, importConfigDTOS, openProductList, extInfoList);
            if (Objects.nonNull(node)) {
                failList.add(nodeInfo);
                continue;
            }
            waitAddNodeExtInfos.put(nodeInfo, extInfoList);
            waitAddNodeInfoList2.add(nodeInfo);
        }

        // 开始导入
        for (Object nodeInfo : waitAddNodeInfoList2) {
            VidBaseInfoDTO baseInfo = baseInfoMap.get(nodeInfo);
            AddNodeReqVo addNodeReqVo = addNodeReqVoMap.get(nodeInfo);
            List<ProductDto> openProductList = openProductListMap.get(nodeInfo);

            // 写入前扩展字段校验
            Long parentVid = addNodeReqVo.getParentVid();
            Integer parentVidType = vidTypeMap.get(addNodeReqVo.getParentVid());

            List<NodeExtInfoDto> extInfoList = waitAddNodeExtInfos.get(nodeInfo);
            Map<Long/* productId */, Map<String/* extFieldKey */, String/* extFieldValue */>> extFieldInfoCheckMap = Maps.newHashMap();
            extInfoList.forEach(extInfo -> extFieldInfoCheckMap.put(extInfo.getProductId(), extInfo.getExtInfoMap()));

            CheckImportExtFieldsResult checkImportExtFieldsResult =
                    importTool.checkImportExtFields(parentVid, parentVidType, extFieldInfoCheckMap);

            if (!Boolean.TRUE.equals(checkImportExtFieldsResult.getSuccess())) {
                setValue(nodeInfo, "error", checkImportExtFieldsResult.getFailMessage());
                failList.add(nodeInfo);
                continue;
            }

            //创建节点并开通产品
            Long vid = null;
            try {
                structureService.checkNodeNameAndDesc(baseInfo, vidType);
                //手动开通模式则传入主产品
                if (activeModel) {
                    List<Long> extendVidProductIdList = openProductList.stream()
                            .filter(ProductDto::getIsMainProduct)
                            .map(ProductDto::getProductId)
                            .collect(toList());
                    addNodeReqVo.setExtendVidProductIdList(extendVidProductIdList);
                }
                HttpResponse<CreateVidVo> addNode = structureService.addNode(addNodeReqVo, null);
                if (!Objects.equals(MerchantErrorCode.HTTP_SUCCESS.getErrorCode(), addNode.getErrcode())) {
                    setValue(nodeInfo, "error", addNode.getErrmsg());
                    failList.add(nodeInfo);
                    continue;
                }
                vid = addNode.getData().getVid();
            } catch (Exception e) {
                setValue(nodeInfo, "error", GlobalErr.getErrorCode().getUsererrmsg());
                failList.add(nodeInfo);
                continue;
            } finally {
                GlobalErr.clearErrorCode();
            }
            // 根据开通成功的产品开写入扩展字段
            List<ProductInstanceFlowDTO> productInstances = productFacade.getVidProductInstanceId(vid, ActivationStatusEnum.IN_USE);
            List<Long> productIdList = FunctionUtil.select(productInstances, ProductInstanceFlowDTO::getProductId);
            List<NodeExtInfoDto> importExtInfoList = FunctionUtil.where(extInfoList, extInfo -> productIdList.contains(extInfo.getProductId()));
            Map<Long/* productId */, Map<String/* extFieldKey */, String/* extFieldValue */>> extFieldInfoMap = Maps.newHashMap();
            importExtInfoList.forEach(extInfo -> extFieldInfoMap.put(extInfo.getProductId(), extInfo.getExtInfoMap()));
            try {
                importTool.saveExtField(vid, vidType, Boolean.TRUE, extFieldInfoMap, wid);
            } catch (MerchantBaseNoStackTraceException e) {
                setValue(nodeInfo, "error", PRODUCT_ID_ERROR.getMessage());
                failList.add(nodeInfo);
                continue;
            }
        }
        resDto.setErrorList(failList);
        return resDto;
    }

    private List<Map<String, Object>> importEditNodes(ImportNodeReqVo reqVo, Map<String, List<Object>> nodeListMap,
                                                      Map<Integer, Class> parseClass, RootNode rootNode) {
        Long bosId = reqVo.getBosId();
        Long wid = reqVo.getWid();
        String rootVidType = mainContentProperties.getVidTypeName(bosId);
        List<Long> vidList = nodeListMap.values().stream().flatMap(Collection::stream).map(node -> {
            String vid = getKey(node, "vid");
            try {
                return StringUtils.isNotBlank(vid) ? Long.parseLong(vid) : null;
            } catch (Exception e) {
                return null;
            }
        }).filter(Objects::nonNull).distinct().collect(toList());
        List<VidInfoDto> vidInfoDtos = structureFacade.selectVidInfo(vidList, bosId);
        Map<Long, VidInfoDto> vidInfoDtoMap = vidInfoDtos.stream()
                .collect(Collectors.toMap(VidInfoDto::getVid, Function.identity()));
        List<Map<String, Object>> failDataList = new ArrayList<>();
        Integer sheetNo = 0;
        List<ProductInstanceFlowDTO> productInstanceId = productFacade.getBosProductInstanceId(bosId);
        List<Long> productIds = productInstanceId.stream()
                .filter(instance -> !BUSINESS_DISTRICT_PRODUCT_ID.equals(instance.getProductId()))
                .map(ProductInstanceFlowDTO::getProductId).distinct().collect(toList());
        List<Long> importVids = new ArrayList<>();
        for (Map.Entry<String, List<Object>> vidTypeNodeEntry : nodeListMap.entrySet()) {
            //初始化校验容器
            VidExtFieldImportTool importTool = vidExtFieldService.instanceImportTool(bosId);
            String vidTypeDesc = vidTypeNodeEntry.getKey();
            Integer vidType;
            if (Objects.nonNull(VidTypeEnum.enumByDesc(vidTypeDesc))) {
                vidType = VidTypeEnum.enumByDesc(vidTypeDesc).getType();
            } else {
                vidType = rootNode.getVidType();
            }
            //查询开通应用,根据模板过滤校验的产品
            //List<ProductDto> productInfo = getProductInfo(bosId, vidType);
            //List<Long> filterProductId = productInfo.stream().map(ProductDto::getProductId).collect(toList());
            List<Object> nodeList = vidTypeNodeEntry.getValue();
            List<ProductNodeExtImportConfigDTO> importConfigDTOS = expandFiled(productIds, vidType);
            for (Object nodeInfo : nodeList) {
                appendExt(bosId, importTool, nodeInfo, importConfigDTOS, productIds);
            }
            //失败列表
            List<Object> failList = new ArrayList<>();
            for (Object nodeInfo : nodeList) {
                if (rootVidType.equals(vidTypeDesc)) {
                    setValue(nodeInfo, "error", EDIT_ROOT_VID_ERROR.getMessage().replaceAll("vidType", rootVidType));
                    failList.add(nodeInfo);
                    continue;
                }
                Object info = validField(nodeInfo);
                if (Objects.nonNull(info)) {
                    failList.add(nodeInfo);
                    continue;
                }
                Long vid;
                try {
                    vid = Long.parseLong(getKey(nodeInfo, "vid"));
                } catch (Exception e) {
                    setValue(nodeInfo, "error", VID_ERROR.getMessage());
                    failList.add(nodeInfo);
                    continue;
                }
                //原节点信息
                VidInfoDto vidInfoDto = vidInfoDtoMap.get(vid);
                if (Objects.isNull(vidInfoDto) || !bosId.equals(vidInfoDto.getBosId())) {
                    setValue(nodeInfo, "error", VID_INFO_ISNULL.getMessage());
                    failList.add(nodeInfo);
                    continue;
                }
                //校验上级组织id不可更改
                String parentVidStr = getKey(nodeInfo, "parentVid");
                if (StringUtils.isNotBlank(parentVidStr)) {
                    if (StringUtils.isNotBlank(parentVidStr)) {
                        Long parentVid = Long.valueOf(parentVidStr.trim());
                        if (!(vidInfoDto.getParentVid() == null || vidInfoDto.getParentVid().equals(parentVid))) {
                            setValue(nodeInfo, "error", VID_PARENT_ERROR.getMessage());
                            failList.add(nodeInfo);
                            continue;
                        }
                    }
                }
                //重复导入校验
                if (importVids.contains(vid)) {
                    setValue(nodeInfo, "error", VID_DUPLICATE_ERROR.getMessage());
                    failList.add(nodeInfo);
                    continue;
                }
                importVids.add(vid);
                if (!vidInfoDto.getVidTypes().contains(vidType)) {
                    setValue(nodeInfo, "error", VID_INFO_ERROR.getMessage());
                    failList.add(nodeInfo);
                    continue;
                }
                //数据组装
                EditNodeReqVo editReqVo = new EditNodeReqVo();
                editReqVo.setBosId(bosId);
                editReqVo.setMerchantId(reqVo.getMerchantId());
                editReqVo.setWid(reqVo.getWid());
                editReqVo.setVid(vid);
                editReqVo.setVidType(vidType);
                VidBaseInfoDTO baseInfo = new VidBaseInfoDTO();
                String vidName = getKey(nodeInfo, "vidName");
                baseInfo.setVidName(StringUtils.isNotBlank(vidName) ? vidName : vidInfoDto.getBaseInfo().getVidName());
                String vidCode = getKey(nodeInfo, "vidCode");
                baseInfo.setVidCode(StringUtils.isNotBlank(vidCode) ? vidCode : vidInfoDto.getBaseInfo().getVidCode());
                VidExtInfosDTO extInfo = new VidExtInfosDTO();
                Object checkResult = checkBaseExtInfo(nodeInfo, vidType, extInfo, baseInfo, vidInfoDto);
                if (Objects.nonNull(checkResult)) {
                    failList.add(checkResult);
                    continue;
                }
                //校验组织权限
                String vidPrivilege = getKey(nodeInfo, "vidPrivilege");
                if (StringUtils.isNotBlank(vidPrivilege)) {
                    NodePrivilegeResVo nodePrivilege = nodePrivilegeService
                            .getNodePrivilege(bosId, reqVo.getVid(), vidType, vidPrivilege);
                    if (Objects.isNull(nodePrivilege)) {
                        setValue(nodeInfo, "error", String.format(NODE_PRIVILEGE_ERROR.getMessage(), vidPrivilege));
                        failList.add(nodeInfo);
                        continue;
                    }
                    editReqVo.setNodePrivilegeId(nodePrivilege.getRoleId());
                }
                //校验组织标签
                if (StringUtils.isNotBlank(getKey(nodeInfo, "vidTags"))) {
                    List<Long> tagIds = Collections.emptyList();
                    tagIds = checkNodeTagIds(bosId, nodeInfo);
                    if (CollectionUtils.isEmpty(tagIds)) {
                        failList.add(nodeInfo);
                        continue;
                    }
                    editReqVo.setNodeTagIds(tagIds);
                }
                editReqVo.setExtInfo(extInfo);
                editReqVo.setBaseInfo(baseInfo);
                // 扩展字段校验及封装
                List<NodeExtInfoDto> extInfoList = new ArrayList<>();
                List<ProductInstanceFlowDTO> vidProductInstanceId = productFacade.getVidProductInstanceId(vid);
                List<ProductDto> openProductList = BeanCopyUtils.copy(vidProductInstanceId, ProductDto.class);
                //openProductList = openProductList.stream().filter(productDto -> filterProductId.contains(productDto.getProductId())).collect(toList());

                Object node = validExt(bosId, importTool, nodeInfo, importConfigDTOS, openProductList, extInfoList);
                if (Objects.nonNull(node)) {
                    failList.add(nodeInfo);
                    continue;
                }
                // 写入前扩展字段校验
                Map<Long/* productId */, Map<String/* extFieldKey */, String/* extFieldValue */>> extFieldInfoCheckMap = Maps.newHashMap();
                extInfoList.forEach(ei -> extFieldInfoCheckMap.put(ei.getProductId(), ei.getExtInfoMap()));

                CheckImportExtFieldsResult checkImportExtFieldsResult =
                        importTool.checkEditExtFields(vid, vidType, extFieldInfoCheckMap);

                if (!Boolean.TRUE.equals(checkImportExtFieldsResult.getSuccess())) {
                    setValue(nodeInfo, "error", checkImportExtFieldsResult.getFailMessage());
                    failList.add(nodeInfo);
                    continue;
                }
                //节点信息编辑
                try {
                    structureService.checkNodeNameAndDesc(baseInfo, vidType);
                    HttpResponse<Boolean> editNodeResponse = structureService.editNode(editReqVo, null);
                    if (!Objects.equals(MerchantErrorCode.HTTP_SUCCESS.getErrorCode(), editNodeResponse.getErrcode())) {
                        setValue(nodeInfo, "error", editNodeResponse.getErrmsg());
                        failList.add(nodeInfo);
                        continue;
                    }
                } catch (Exception e) {
                    setValue(nodeInfo, "error", GlobalErr.getErrorCode().getUsererrmsg());
                    failList.add(nodeInfo);
                    continue;
                } finally {
                    GlobalErr.clearErrorCode();
                }
                //扩展字段编辑
                extInfoList = FunctionUtil.where(extInfoList, ext -> productIds.contains(ext.getProductId()));
                Map<Long/* productId */, Map<String/* extFieldKey */, String/* extFieldValue */>> extFieldInfoMap = Maps.newHashMap();
                extInfoList.forEach(ext -> extFieldInfoMap.put(ext.getProductId(), ext.getExtInfoMap()));
                try {
                    importTool.saveExtField(vid, vidType, Boolean.FALSE, extFieldInfoMap, wid);
                } catch (MerchantBaseNoStackTraceException e) {
                    setValue(nodeInfo, "error", PRODUCT_ID_ERROR.getMessage());
                    failList.add(nodeInfo);
                    continue;
                }
            }
            if (CollectionUtils.isNotEmpty(failList)) {
                Map<String, Object> failMap = new HashMap<>();
                failMap.put("sheetNo", sheetNo);
                failMap.put("sheetName", vidTypeDesc);
                failMap.put("entity", parseClass.get(vidType));
                failMap.put("data", failList);
                failDataList.add(failMap);
                sheetNo++;
            }
        }
        return failDataList;
    }

    /**
     * 下载模板生成示例数据
     *
     * @param clazz
     * @return
     */
    @Override
    public List<Object> exportExample(Class<T> clazz) {
        List<Object> dataList = new ArrayList<>();
        Object object = null;
        try {
            object = clazz.newInstance();
        } catch (Exception e) {
            return dataList;
        }
        List<String> fields = Lists.newArrayList(clazz.getSuperclass().getDeclaredFields()).stream()
                .map(Field::getName).collect(toList());
        for (NodeImportExampleData exampleData : NodeImportExampleData.values()) {
            if (fields.contains(exampleData.getKey())) {
                setValue(object, exampleData.getKey(), exampleData.getDesc());
            }
        }
        dataList.add(object);
        return dataList;
    }

    /**
     * 获取扩展字段标题
     *
     * @return
     */
    private List<ProductNodeExtImportConfigDTO> expandFiled(List<Long> productIds, Integer vidType) {
        if (CollectionUtils.isEmpty(productIds)) {
            return Lists.newArrayList();
        }
        ProductNodeExtImportConfigQuery query = ProductNodeExtImportConfigQuery.builder()
                .productIds(productIds)
                .vidType(vidType)
                .build();
        SoaResponse<List<ProductNodeExtImportConfigDTO>, Void> soaResponse = nodeExtConfigAbility
                .queryProductNodeExtImportConfigList(query);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        return soaResponse.getResponseVo();
    }

    /**
     * 查询spi实现扩展点
     */
    private List<String> getExtFieldKeyBySpi(Long bosId, Long productId, Long productInstanceId, List<String> extFieldKeys, String businessDomain) {
        return extFieldSpiService.getUsableExtFieldKeys(bosId, productId, productInstanceId, extFieldKeys, businessDomain);
    }

    /**
     * 查询节点类型下按产品分组的扩展字段
     *
     * @param bosId
     * @return
     */
    private List<ExportVidExtFieldConfigDTO> getAllFieldKeyList(Long bosId) {
        List<ExportVidExtFieldConfigDTO> extFieldConfigList = new ArrayList<>();

        // 查询全部产品实例
        List<ProductInstanceFlowDTO> bosProductInstanceId = productFacade.getBosProductInstanceId(bosId);

        // 排除店铺产品
        List<Long> productIds = bosProductInstanceId.stream()
                .map(ProductInstanceFlowDTO::getProductId)
                .filter(productId -> !Constants.DEFAULT_PRODUCT_ID.equals(productId))
                .collect(toList());

        Map<Long, Long> productMap = bosProductInstanceId.stream()
                .collect(toMap(ProductInstanceFlowDTO::getProductId, ProductInstanceFlowDTO::getProductInstanceId));

        List<Integer> vidTypeList = getBosVidTypes(bosId);
        for (Integer vidType : vidTypeList) {
            List<ProductNodeExtImportConfigDTO> configDTOS = expandFiled(productIds, vidType);

            // 文本，单选，多选，非级联字段才支持导出
            configDTOS = FunctionUtil.where(configDTOS, config -> {
                if (Objects.equals(config.getFieldType(), 0)) {
                    return true;
                } else if (Objects.equals(config.getFieldType(), 1) && !Objects.equals(config.getFieldCascade(), 0)) {
                    return true;
                } else if (Objects.equals(config.getFieldType(), 2) && !Objects.equals(config.getFieldCascade(), 0)) {
                    return true;
                }
                return false;
            });

            if (CollectionUtils.isNotEmpty(configDTOS)) {
                Map<Long, List<ProductNodeExtImportConfigDTO>> productExtMap = configDTOS.stream()
                        .collect(Collectors.groupingBy(ProductNodeExtImportConfigDTO::getProductId));

                productExtMap.forEach((productId, configList) -> {
                    Long productInstanceId = productMap.get(productId);
                    // 按域拆分
                    Map<String/* businessDomain */, List<ProductNodeExtImportConfigDTO>> businessDomainConfigsMap =
                            FunctionUtil.selectListMap(configList, config -> config.getFieldDomain() != null ? config.getFieldDomain() : "");

                    // 内部
                    List<String> extFieldKeys = new ArrayList<>();
                    // 外部
                    List<String> withoutExtFieldKeys = new ArrayList<>();
                    Map<String/* withoutExtFieldKey */, String/* businessDomain */> withoutExtFieldKeyBusinessDomainMap = Maps.newHashMap();

                    businessDomainConfigsMap.forEach((businessDomain, configs) -> {
                        List<String> keyList = FunctionUtil.select(configs, ProductNodeExtImportConfigDTO::getFieldKey);
                        keyList = getExtFieldKeyBySpi(bosId, productId, productInstanceId, keyList, businessDomain);

                        Map<String/* extFieldKey */, ProductNodeExtImportConfigDTO> extFieldKeyConfigMap =
                                FunctionUtil.selectMap(configList, ProductNodeExtImportConfigDTO::getFieldKey);
                        // 拆分内部还是外部
                        keyList.forEach(key -> {
                            ProductNodeExtImportConfigDTO config = extFieldKeyConfigMap.get(key);
                            if (Boolean.TRUE.equals(config.getNeedCheck())) {
                                withoutExtFieldKeys.add(key);
                                withoutExtFieldKeyBusinessDomainMap.put(key, config.getFieldDomain() != null ? config.getFieldDomain() : "");
                            } else {
                                extFieldKeys.add(key);
                            }
                        });
                    });

                    ExportVidExtFieldConfigDTO extFieldConfig = new ExportVidExtFieldConfigDTO();
                    extFieldConfig.setVidType(vidType);
                    extFieldConfig.setProductId(productId);
                    extFieldConfig.setProductInstanceId(productInstanceId);
                    extFieldConfig.setExtFieldKeys(extFieldKeys);
                    extFieldConfig.setWithoutExtFieldKeys(withoutExtFieldKeys);
                    extFieldConfig.setWithoutExtFieldKeyBusinessDomainMap(withoutExtFieldKeyBusinessDomainMap);
                    extFieldConfigList.add(extFieldConfig);
                });
            }
        }
        return extFieldConfigList;
    }

    /**
     * 获取bos下已存在的组织类型
     *
     * @param bosId
     * @return
     */
    private List<Integer> getBosVidTypes(Long bosId) {
        GetVidTypeByBosReqDTO reqDTO = new GetVidTypeByBosReqDTO();
        reqDTO.setBosId(bosId);
        SoaResponse<GetVidTypeByBosRespDTO, Void> vidTypeByBos = bosExportService.getVidTypeByBos(reqDTO);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(vidTypeByBos), MpBizCode.build(vidTypeByBos));
        return vidTypeByBos.getResponseVo().getVidTypes();
    }


    /**
     * 白名单用户表头
     *
     * @return
     */
    private List<ProductDto> getProductInfo(Long bosId, Integer vidType) {
        List<ProductDto> productList = new ArrayList<>();
        InstanceForActiveResVo resVo = productService.queryInstanceListForActive(new QueryInstanceListForActiveReqVo(bosId, vidType));
        if (CollectionUtils.isEmpty(resVo.getMainProductInstanceList())) {
            return productList;
        }
        for (MainProductInstanceDto instanceDto : resVo.getMainProductInstanceList()) {
            if (!instanceDto.getIsOpen()) {
                continue;
            }
            ProductDto productDto = BeanCopyUtils.copy(instanceDto, ProductDto.class);
            productDto.setIsMainProduct(Boolean.TRUE);
            List<ProductDto> followProductList = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(instanceDto.getFollowInstanceList())) {
                followProductList.addAll(BeanCopyUtils.copy(instanceDto.getFollowInstanceList(), ProductDto.class));
            }
            if (instanceDto.getIsHadMarketing()) {
                followProductList.addAll(BeanCopyUtils.copy(instanceDto.getInstanceForActiveGroupDto().getProductInstanceList(), ProductDto.class));
            }
            productDto.setFollowProductList(followProductList);
            productList.add(productDto);
        }
        return productList;
    }

    /**
     * @return
     */
    private List<ProductInstanceFlowDTO> checkExpandFiledForBos(Long bosId, Integer vidType) {
        List<ProductInstanceFlowDTO> productIds = new ArrayList<>();
        //查询bosId级别的产品应用
        List<ProductInstanceFlowDTO> productInstanceIdList = productFacade.getBosProductInstanceId(bosId);
        if (CollectionUtils.isEmpty(productInstanceIdList)) {
            return productIds;
        }
        //过滤掉 320L产品实例id
        List<Long> productInstanceIds = productInstanceIdList.stream()
                .filter(instance -> !BUSINESS_DISTRICT_PRODUCT_ID.equals(instance.getProductId()))
                .map(ProductInstanceFlowDTO::getProductInstanceId)
                .distinct().collect(toList());
        //获取应用对应适配的节点类型分别过滤
        Map<Long, Map<String, Object>> functionMap = productFacade.batchQueryAttachFunctionByKeyList
                (productInstanceIds, Arrays.asList(PublicAuxiliaryFunctionEnum.ADAPTER_VID_TYPE_OPTION.getKey()));
        Set<Long> productInstanceIdSet = new HashSet<>(productInstanceIds.size());
        functionMap.keySet().forEach(key -> {
            Map<String, Object> valueMap = functionMap.get(key);
            if (Objects.isNull(valueMap)) {
                return;
            }
            Object adapterVidType = valueMap.get(PublicAuxiliaryFunctionEnum.ADAPTER_VID_TYPE_OPTION.getKey());
            if (Objects.isNull(adapterVidType)) {
                return;
            }
            List<Integer> vidTypeList = JSONArray.parseArray(adapterVidType.toString(), Integer.class);
            if (vidTypeList.contains(vidType)) {
                productInstanceIdSet.add(key);
            }
        });

        for (ProductInstanceFlowDTO dto : productInstanceIdList) {
            if (productInstanceIdSet.contains(dto.getProductInstanceId())) {
                productIds.add(dto);
            }
        }
        productIds = productIds.stream().distinct().collect(toList());
        return productIds;
    }

    private Object generateObject(List<String> fieldList, Class templateClass, Boolean isImport, Set<String> excelAnnFields) throws Exception {
        ClassPool pool = ClassPool.getDefault();
        CtClass templateCtClass = pool.get(templateClass.getName());
        if (templateCtClass.isFrozen()) {
            templateCtClass.defrost();
        }
        String ctClassName = "cn." + templateClass.getName() + "ClassImp";
        CtClass ctClass;
        try {
            CtClass poolClass = pool.get(ctClassName);
            ArrayList<CtField> ctFields = Lists.newArrayList(poolClass.getDeclaredFields());
            ArrayList<CtMethod> ctMethods = Lists.newArrayList(poolClass.getDeclaredMethods());
            if (poolClass.isFrozen()) {
                poolClass.defrost();
            }
            for (CtField ctField : ctFields) {
                poolClass.removeField(ctField);
            }
            for (CtMethod ctMethod : ctMethods) {
                poolClass.removeMethod(ctMethod);
            }
            ctClass = poolClass;
        } catch (javassist.NotFoundException e) {
            ctClass = pool.makeClass(ctClassName, templateCtClass);
        } catch (RuntimeException e) {
            ctClass = pool.makeClass(ctClassName, templateCtClass);
        }
        ConstPool constPool = ctClass.getClassFile().getConstPool();
        if (isImport) {
            fieldList.add("error");
        }
        List<String> ctFields = Lists.newArrayList(ctClass.getDeclaredFields()).stream().map(CtField::getName).collect(toList());
        for (String field : fieldList) {
            if (ctFields.contains(field)) {
                continue;
            }
            // 创建新字段
            CtField ctField = CtField.make("public String " + field + ";", ctClass);
            ctField.setModifiers(Modifier.PUBLIC);
            //字段新增属性
            AnnotationsAttribute fieldAttr = new AnnotationsAttribute(constPool, AnnotationsAttribute.visibleTag);
            Annotation annotation1 = new Annotation(ExcelProperty.class.getName(), constPool);
            fieldAttr.addAnnotation(annotation1);
            if (excelAnnFields.contains(field)) {
                Annotation annotation2 = new Annotation(ExcelAnnotation.class.getName(), constPool);
                fieldAttr.addAnnotation(annotation2);
            }
            ctField.getFieldInfo().addAttribute(fieldAttr);
            ctClass.addField(ctField);
            char[] cs = field.toCharArray();
            cs[0] -= 32;
            String upperField = String.valueOf(cs);
            String set = "public void set" + upperField + "(String " + field + "){this." + field + " = " + field + ";}";
            String get = "public String get" + upperField + "(){return this." + field + ";}";
            CtMethod getMethod = CtMethod.make(get, ctClass);
            CtMethod setMethod = CtMethod.make(set, ctClass);
            ctClass.addMethod(getMethod);
            ctClass.addMethod(setMethod);
        }
        String path = this.getClass().getResource("").getPath();
        NodeImportClassLoader nodeImportClassLoader = new NodeImportClassLoader(path);
        ctClass.writeFile(path);
        Class<?> aClass = nodeImportClassLoader.loadClass(ctClass.getName());
        Object newObject = aClass.newInstance();
        return newObject;
    }

    /**
     * 获取固定字段属性
     *
     * @param bean
     * @param field
     * @return
     */
    private String getKey(Object bean, String field) {
        BeanMap beanMap = BeanMap.create(bean);
        String fieldValue = (String) beanMap.get(field);
        return fieldValue;
    }

    @SneakyThrows
    private void setValue(Object bean, String field, String value) {
        BeanUtils.setProperty(bean, field, value);
    }

    private List<VidInfoDto> convertVidCodeAndType(Long bosId, List<Object> nodeInfoList) {
        List<Long> parentVids = nodeInfoList.stream()
                .filter(dto -> {
                    if (StringUtils.isNotBlank(getKey(dto, "parentVid"))) {
                        try {
                            Long.parseLong(getKey(dto, "parentVid"));
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }
                    return false;
                }).map(node -> Long.parseLong(getKey(node, "parentVid"))).distinct().collect(toList());
        return structureFacade.selectVidInfo(parentVids, bosId);
    }

    /**
     * 地址解析
     *
     * @param address
     * @return
     */
    private SoaResponse<ParsedDivisionBO, AddressCommonError> parseAddress(String address) {
        DivisionRequest divisionRequest = new DivisionRequest();
        divisionRequest.setSource(SourceEnum.MP_USER.getSource());
        divisionRequest.setMapSystemCode(MapSystemEnum.AMAP.getCode());
        divisionRequest.setAddress(address);
        SoaResponse<ParsedDivisionBO, AddressCommonError> soa = administrativeDivisionService.parseAddress(divisionRequest);
        return soa;
    }

    /**
     * 校验营业时间格式是否正确
     * 每天：08:00-12:00、13:00-22:00；
     * 周一、周二：08:00-12:00、13:00-22:00；
     * 周三、周四、周五：08:00-12:00、13:00-14:00；
     *
     * @param businessTime
     * @return
     */
    private List<BusinessTimeDTO> parseBusinessTime(String businessTime) {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm");
        businessTime = businessTime.replace("；", ";").replace("：", ":").replace("\n", "");
        String[] timeArr = businessTime.split(";");
        List<BusinessTimeDTO> businessTimeList = new ArrayList<>();
        try {
            if (timeArr.length == 1 && businessTime.startsWith(BusinessTimeEnum.EVERYDAY.getTime())) {
                String[] hourArr = timeArr[0].substring(timeArr[0].indexOf(":") + 1, timeArr[0].length()).split("、");
                for (int i = 0; i < hourArr.length; i++) {
                    Date startTime = format.parse(hourArr[i].split("-")[0]);
                    Date endTime = format.parse(hourArr[i].split("-")[1]);
                    if (startTime.after(endTime)) {
                        return Collections.emptyList();
                    }
                }
                BusinessTimeDTO businessTimeDto = new BusinessTimeDTO();
                businessTimeDto.setBusinessDays(BusinessTimeEnum.getWeekNums());
                businessTimeDto.setBusinessHours(Arrays.asList(hourArr));
                businessTimeDto.setBusinessDesc(BusinessTimeEnum.EVERYDAY.getTime());
                businessTimeList.add(businessTimeDto);
            }

            if (timeArr.length > 1 || (timeArr.length == 1 && !businessTime.startsWith(BusinessTimeEnum.EVERYDAY.getTime()))) {
                for (int i = 0; i < timeArr.length; i++) {
                    List<Integer> businessDays = new ArrayList<>();
                    String[] weekArr = timeArr[i].substring(0, timeArr[i].indexOf(":")).split("、");
                    for (int j = 0; j < weekArr.length; j++) {
                        if (!BusinessTimeEnum.getWeeks().contains(weekArr[j])) {
                            return Collections.emptyList();
                        }
                        businessDays.add(BusinessTimeEnum.getNumByTime(weekArr[j]));
                    }
                    String[] hourArr = timeArr[i].substring(timeArr[i].indexOf(":") + 1, timeArr[i].length()).split
                            ("、");
                    for (int z = 0; z < hourArr.length; z++) {
                        Date startTime = format.parse(hourArr[z].substring(0, hourArr[z].indexOf("-")));
                        Date endTime = format.parse(hourArr[z].split("-")[1]);
                        if (startTime.after(endTime)) {
                            return Collections.emptyList();
                        }
                    }
                    BusinessTimeDTO businessTimeDto = new BusinessTimeDTO();
                    businessTimeDto.setBusinessDays(businessDays);
                    businessTimeDto.setBusinessHours(Arrays.asList(hourArr));
                    businessTimeDto.setBusinessDesc(BusinessTimeEnum.EVERYWEEK.getTime());
                    businessTimeList.add(businessTimeDto);
                }
            }
        } catch (Exception e) {
            return Collections.emptyList();
        }
        if (businessTimeList.size() != timeArr.length) {
            return Collections.emptyList();
        }
        return businessTimeList;
    }

    @SneakyThrows
    private <T> T validField(T nodeInfo) {
        List<Field> fields = new ArrayList<>();
        fields.addAll(Arrays.asList(nodeInfo.getClass().getDeclaredFields()));
        fields.addAll(Arrays.asList(nodeInfo.getClass().getSuperclass().getDeclaredFields()));
        for (Field f : fields) {
            if (f.isAnnotationPresent(NotAllowNull.class)) {
                NotAllowNull notAllowNull = f.getAnnotation(NotAllowNull.class);
                Class<?>[] enumCheck = notAllowNull.enumCheck();
                Method setError = nodeInfo.getClass().getMethod("setError", String.class);
                //字段名称
                ExcelProperty excelProperty = f.getAnnotation(ExcelProperty.class);
                String fileName = excelProperty.value()[1].replace("*", "");

                try {
                    String fielValue = (String) f.get(nodeInfo);
                    //必填校验
                    if (StringUtils.isBlank(fielValue)) {
                        setError.invoke(nodeInfo, notAllowNull.impotErr().getMessage());
                        return nodeInfo;
                    }
                    //枚举校验
                    if (enumCheck.length > 0) {
                        Method enumByDesc = enumCheck[0].getDeclaredMethod("enumByDesc", String.class);
                        Object[] enus = enumCheck[0].getEnumConstants();

                        Object invoke = null;
                        for (Object enu : enus) {
                            invoke = enumByDesc.invoke(enu, fielValue);
                        }
                        if (Objects.isNull(invoke)) {
                            setError.invoke(nodeInfo, fileName + DATA_ERROR.getMessage());
                            return nodeInfo;
                        }
                    }
                } catch (Exception e) {
                    setError.invoke(nodeInfo, PARSE_ERROR.getMessage());
                    return nodeInfo;
                }

            }
        }
        return null;
    }

    /**
     * 校验基础扩展字段
     *
     * @param nodeInfo
     * @param vidType
     * @return
     */
    private Object checkBaseExtInfo(Object nodeInfo, Integer vidType, VidExtInfosDTO extInfo, VidBaseInfoDTO baseInfo
            , VidInfoDto vidInfoDto) {
        //品牌/门店/商场/自提点/网点填写的信息
        List<Integer> checkVidType = new ArrayList<>();
        checkVidType.add(VidTypeEnum.STORE.getType());
        checkVidType.add(VidTypeEnum.SELF_PICKUP_SITE.getType());
        checkVidType.add(VidTypeEnum.MALL.getType());
        checkVidType.add(VidTypeEnum.OUTLETS.getType());
        checkVidType.add(VidTypeEnum.BRAND.getType());
        if (checkVidType.contains(vidType)) {
            Map<String, String> addressExtInfoMap = new HashMap<>();
            String provinceName = getKey(nodeInfo, "provinceName");
            if (StringUtils.isNotBlank(provinceName)) {
                addressExtInfoMap.put(ExtFieldKeyEnum.ADDRESS_PROVINCE_NAME.getKey(),
                        provinceName);
            }
            String cityName = getKey(nodeInfo, "cityName");
            if (StringUtils.isNotBlank(cityName)) {
                addressExtInfoMap.put(ExtFieldKeyEnum.ADDRESS_CITY_NAME.getKey(),
                        cityName);
            }
            String countryName = getKey(nodeInfo, "countryName");
            if (StringUtils.isNotBlank(countryName)) {
                addressExtInfoMap.put(ExtFieldKeyEnum.ADDRESS_COUNTY_NAME.getKey(),
                        countryName);
            }
            String detailAddress = getKey(nodeInfo, "detailAddress");
            if (StringUtils.isNotBlank(detailAddress)) {
                addressExtInfoMap.put(ExtFieldKeyEnum.ADDRESS_DETAIL.getKey(),
                        detailAddress);
            }
            if (!checkAddress(addressExtInfoMap)) {
                setValue(nodeInfo, "error", BackstageErrorCode.IMPORT_ADDRESS_ERROR.getErrorMsg());
                return nodeInfo;
            }
            Object buildAddress = buildAddress(addressExtInfoMap, nodeInfo, vidInfoDto, extInfo);
            if (Objects.nonNull(buildAddress)) {
                return buildAddress;
            }
            //校验营业时间
            String businessTime = getKey(nodeInfo, "businessTime");
            if (StringUtils.isNotBlank(businessTime)) {
                List<BusinessTimeDTO> businessTimeDtos = null;
                if (VidTypeEnum.BRAND.getType().equals(vidType)) {
                    businessTimeDtos = new ArrayList<>();
                }
                if (CollectionUtils.isEmpty(businessTimeDtos = parseBusinessTime(businessTime))) {
                    setValue(nodeInfo, "error", BUSINESSTIME_ERROR.getMessage());
                    return nodeInfo;
                }
                extInfo.setBusinessTime(businessTimeDtos);
            }
            //校验手机号
            String contactTels = getKey(nodeInfo, "contactTels");
            if (StringUtils.isNotBlank(contactTels)) {
                boolean match = ValidateUtil.match(ValidateContants.NODE_TEL, contactTels);
                if (!match) {
                    setValue(nodeInfo, "error", CONTACTTELS_ERROR.getMessage());
                    return nodeInfo;
                }
                extInfo.setContactTel(contactTels);
            }

            //校验logo
            String isUnifyLogo = getKey(nodeInfo, "isUnifyLogo");
            if (StringUtils.isNotBlank(isUnifyLogo)) {
                if (Objects.isNull(IsUnifyLogoEnum.enumByDesc(isUnifyLogo))) {
                    isUnifyLogo = isUnifyLogo.replaceAll(VidTypeEnum.enumByCode(vidType).getDesc(), VidTypeEnum.STORE.getDesc());
                    if (Objects.isNull(IsUnifyLogoEnum.enumByDesc(isUnifyLogo))) {
                        setValue(nodeInfo, "error", LOGO_ERROR.getMessage());
                        return nodeInfo;
                    }
                }
                if (IsUnifyLogoEnum.IS_ALONE.getDesc().equals(isUnifyLogo)) {
                    if (StringUtils.isBlank(getKey(nodeInfo, "vidLogo"))) {
                        setValue(nodeInfo, "error", VIDLOGO_ISNULL.getMessage());
                        return nodeInfo;
                    }
                    if (!UrlValidUtil.validImage(getKey(nodeInfo, "vidLogo"))) {
                        setValue(nodeInfo, "error", isUnifyLogo + VIDLOGO_ERROR.getMessage());
                        return nodeInfo;
                    }
                }
                extInfo.setIsUnifyLogo(IsUnifyLogoEnum.enumByDesc(isUnifyLogo));
            }
            //校验节点图片
            if (Objects.nonNull(getKey(nodeInfo, "vidPictures"))) {
                log.info("vidPictureskey:" + getKey(nodeInfo, "vidPictures"));
                String[] vidPictures = getKey(nodeInfo, "vidPictures").split(";\n");
                for (int i = 0; i < vidPictures.length; i++) {
                    if (!UrlValidUtil.validImage(vidPictures[i])) {
                        log.error("vidPictures:" + vidPictures[i]);
                        setValue(nodeInfo, "error", VIDPICTURES_ERROR.getMessage());
                        return nodeInfo;
                    }
                }
                extInfo.setVidPictures(Arrays.asList(vidPictures));
            }
            //校验面积
            if (StringUtils.isNotBlank(getKey(nodeInfo, "size"))) {
                Double size;
                try {
                    size = Double.valueOf(getKey(nodeInfo, "size"));
                } catch (Exception e) {
                    setValue(nodeInfo, "error", SIZE_ERROR.getMessage());
                    return nodeInfo;
                }
                extInfo.setSize(size);
            }
            //校验节点logo
            if (Objects.nonNull(getKey(nodeInfo, "vidLogo"))) {
                baseInfo.setVidLogos(Arrays.asList(getKey(nodeInfo, "vidLogo")));
            }
        }
        if (vidType.equals(VidTypeEnum.FLOOR.getType())) {
            if (!FloorNameEnum.getFloors().contains(getKey(nodeInfo, "floorName"))) {
                setValue(nodeInfo, "error", FLOORNAME_ERROR.getMessage());
                return nodeInfo;
            }
            extInfo.setFloorName(getKey(nodeInfo, "floorName"));
            baseInfo.setSort(FloorNameEnum.getFloorByName(getKey(nodeInfo, "floorName")).getFloor());
        }
        String vidStatusDesc = getKey(nodeInfo, "vidStatus");
        if (StringUtils.isNotBlank(vidStatusDesc)) {
            VidStatusEnum statusEnum = VidStatusEnum.enumByDesc(vidStatusDesc);
            if (statusEnum == null) {
                setValue(nodeInfo, "error", VIDSTATUS_REEOR.getMessage());
                return nodeInfo;
            }
            baseInfo.setVidStatus(statusEnum.getId());
        }
        String wxOnlineStatusDesc = getKey(nodeInfo, "wxOnlineStatus");
        if (StringUtils.isNotBlank(wxOnlineStatusDesc)) {
            WxOnlineStatusEnum wxOnlineStatusEnum = WxOnlineStatusEnum.enumByDesc(wxOnlineStatusDesc);
            if (wxOnlineStatusEnum == null) {
                setValue(nodeInfo, "error", WX_ONLINE_STATUS_REEOR.getMessage());
                return nodeInfo;
            }
            extInfo.setWxOnlineStatus(wxOnlineStatusEnum.getId());
        }
        return null;
    }

    /**
     * 组织标签校验
     */
    private List<Long> checkNodeTagIds(Long bosId, Object nodeInfo) {
        FindBosTagByNameReqDTO reqDTO = new FindBosTagByNameReqDTO();
        reqDTO.setBosId(bosId);
        List<String> tagNames = new ArrayList<>(Arrays.asList(getKey(nodeInfo, "vidTags").replace("；", ";").split(";")));
        reqDTO.setTagNames(tagNames);
        SoaResponse<FindBosTagByNameRespDTO, Void> dtoVoidSoaResponse =
                nodeTagExportService.findBosTagByName(reqDTO);
        if (!SoaRespUtils.isSuccess(dtoVoidSoaResponse) || Objects.isNull(dtoVoidSoaResponse.getResponseVo())
                || CollectionUtils.isEmpty(dtoVoidSoaResponse.getResponseVo().getTagList())) {
            setValue(nodeInfo, "error", String.format(DATA_TAG_ERROR.getMessage(), String.join(";", tagNames)));
            return null;
        }
        List<BosTagDTO> tags = dtoVoidSoaResponse.getResponseVo().getTagList();
        tagNames.removeAll(tags.stream().map(BosTagDTO::getTagName).collect(toList()));
        if (CollectionUtils.isNotEmpty(tagNames)) {
            String errorTag = tagNames.stream().collect(Collectors.joining(";"));
            setValue(nodeInfo, "error", String.format(DATA_TAG_ERROR.getMessage(), errorTag));
            return null;
        }
        return tags.stream().map(BosTagDTO::getTagId).collect(toList());
    }

    /**
     * 根据类型获取基础导入模板
     * 默认门店
     *
     * @param vidType
     * @return
     */
    private Class getTemplateClass(Integer vidType, Boolean isImportTemplate) {
        if (vidType.equals(VidTypeEnum.GROUP.getType())) {
            return isImportTemplate ? ImportGroupNodeInfo.class : ExportGroupNodeInfo.class;
        }
        if (vidType.equals(VidTypeEnum.BRAND.getType())) {
            return isImportTemplate ? ImportBrandNodeInfo.class : ExportBrandNodeInfo.class;
        }
        if (vidType.equals(VidTypeEnum.DISTRICT.getType())) {
            return isImportTemplate ? ImportDistrictNodeInfo.class : ExportDistrictNodeInfo.class;
        }
        if (vidType.equals(VidTypeEnum.MALL.getType())) {
            return isImportTemplate ? ImportMallNodeInfo.class : ExportMallNodeInfo.class;
        }
        if (vidType.equals(VidTypeEnum.FLOOR.getType())) {
            return isImportTemplate ? ImportFloorNodeInfo.class : ExportFloorNodeInfo.class;
        }
        if (vidType.equals(VidTypeEnum.SELF_PICKUP_SITE.getType())) {
            return isImportTemplate ? ImportPickupNodeInfo.class : ExportPickupNodeInfo.class;
        }
        if (vidType.equals(VidTypeEnum.OUTLETS.getType())) {
            return isImportTemplate ? ImportOutLetsNodeInfo.class : ExportOutLetsNodeInfo.class;
        }
        if (vidType.equals(VidTypeEnum.DEPARTMENT.getType())) {
            return isImportTemplate ? ImportFloorDepartmentNodeInfo.class : ExportFloorDepartmentNodeInfo.class;
        }
        return isImportTemplate ? ImportNodeInfo.class : ExportNodeInfo.class;
    }

    /**
     * 导入填充扩展字段
     *
     * @param bosId
     * @param nodeInfo
     * @param importConfigDTOS
     * @param openProductList
     * @return
     */
    private void appendExt(Long bosId, VidExtFieldImportTool importTool, Object nodeInfo,
                           List<ProductNodeExtImportConfigDTO> importConfigDTOS, List<Long> openProductList) {
        Map<Long, List<ProductNodeExtImportConfigDTO>> productExtMap = FunctionUtil.selectListMap(importConfigDTOS, ProductNodeExtImportConfigDTO::getProductId);
        for (Map.Entry<Long, List<ProductNodeExtImportConfigDTO>> configEntry : productExtMap.entrySet()) {
            Long productId = configEntry.getKey();
            List<ProductNodeExtImportConfigDTO> configList = configEntry.getValue();
            if (!openProductList.contains(productId)) {
                continue;
            }
            for (ProductNodeExtImportConfigDTO configDTO : configList) {
                String fieldKey = configDTO.getFieldKey();
                String fieldValue = getKey(nodeInfo, fieldKey + productId);

                // 填充tool数据
                importTool.addImportExtFieldText(productId, fieldKey, fieldValue);
            }
        }
    }

    /**
     * 导出填充扩展字段
     *
     * @param bosId
     * @return
     */
    private void exportAppendExt(Long bosId, VidExtFieldExportTool exportTool, List<VidPageInfoDTO> vidInfoList) {
        List<NodeExtInfoDto> nodeExtInfoList = vidInfoList.stream()
                .map(VidPageInfoDTO::getExtInfos)
                .filter(CollectionUtils::isNotEmpty)
                .flatMap(Collection::stream)
                .filter(extInfo -> !Constants.DEFAULT_PRODUCT_ID.equals(extInfo.getProductId()))
                .collect(toList());
        for (NodeExtInfoDto nodeExtInfoDto : nodeExtInfoList) {
            Long productId = nodeExtInfoDto.getProductId();
            nodeExtInfoDto.getExtInfoMap().forEach((extFieldKey, extFieldValue) -> {
                exportTool.addExportExtFieldValue(productId, extFieldKey, extFieldValue);
            });
        }
    }

    /**
     * 校验扩展字段
     *
     * @param bosId
     * @param importTool
     * @param nodeInfo
     * @param importConfigDTOS
     * @param openProductList
     * @param extInfoList
     * @return
     */
    private Object validExt(Long bosId, VidExtFieldImportTool importTool, Object
            nodeInfo, List<ProductNodeExtImportConfigDTO> importConfigDTOS, List<ProductDto> openProductList,
                            List<NodeExtInfoDto> extInfoList) {
        Map<Long, List<ProductNodeExtImportConfigDTO>> productExtMap = FunctionUtil.selectListMap(importConfigDTOS, ProductNodeExtImportConfigDTO::getProductId);
        Map<Long, ProductDto> openProductMap = FunctionUtil.selectMap(openProductList, ProductDto::getProductId);
        for (Map.Entry<Long, List<ProductNodeExtImportConfigDTO>> configEntry : productExtMap.entrySet()) {
            Long productId = configEntry.getKey();
            List<ProductNodeExtImportConfigDTO> configList = configEntry.getValue();
            if (!openProductMap.containsKey(productId)) {
                continue;
            }

            //扩展字段封装
            NodeExtInfoDto nodeExtInfoDto = new NodeExtInfoDto();
            nodeExtInfoDto.setProductId(productId);
            nodeExtInfoDto.setProductInstanceId(openProductMap.get(productId).getProductInstanceId());
            Map<String, String> extInfoMap = new HashMap<>();
            for (ProductNodeExtImportConfigDTO configDTO : configList) {
                String fieldKey = configDTO.getFieldKey();
                String fieldName = configDTO.getFieldName();
                String fieldValue = getKey(nodeInfo, fieldKey + productId);
                try {
                    extInfoMap.put(fieldKey, importTool.getImportExtFieldValue(productId, fieldKey, fieldValue));
                } catch (MerchantBaseNoStackTraceException e) {
                    // 必填项
                    if (e.getErrorCode().equals(BackstageErrorCode.IMPORT_ERROR_EXT_FIELD_REQUIRE.getErrorCode())) {
                        setValue(nodeInfo, "error", configDTO.getFieldName() + NOTNULL_ERROR.getMessage());
                        return nodeInfo;
                    } else if (e.getErrorCode().equals(BackstageErrorCode.IMPORT_ERROR_CONVERSION.getErrorCode())) {
                        setValue(nodeInfo, "error", fieldName + URLCHECK_FAIL.getMessage());
                        return nodeInfo;
                    }

                    setValue(nodeInfo, "error", fieldName + URLCHECK_FAIL.getMessage());
                    return nodeInfo;
                } catch (Exception e) {
                    setValue(nodeInfo, "error", fieldName + URLCHECK_FAIL.getMessage());
                    return nodeInfo;
                }
            }

            nodeExtInfoDto.setExtInfoMap(extInfoMap);

            extInfoList.add(nodeExtInfoDto);
        }
        return null;
    }

    private Boolean checkAddress(Map<String, String> extInfoMap) {
        int count = addressKeySet.size();
        for (String key : addressKeySet) {
            String value = extInfoMap.get(key);
            if (StringUtils.isNotBlank(value)) {
                count--;
            }
        }
        if (count == addressKeySet.size() || count <= 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ImportResDto<ImporSelectNodeInfo> imporSelectNode
            (List<ImporSelectNodeInfo> nodeList, ImporSelectNodeReqVo reqVo) {
        ExceptionUtils.checkState(nodeList.size() <= 5000, MpBizCode.IMPORT_MAX_ERROR);
        List<ImporSelectNodeInfo> errorList = new ArrayList<>();
        List<ImporSelectNodeInfo> successList = new ArrayList<>();
        List<String> vids = new ArrayList<>();
        List<String> codes = new ArrayList<>();
        List<String> msg = new ArrayList<>();
        ImportResDto<ImporSelectNodeInfo> resDto = new ImportResDto<>();
        resDto.setTotal(nodeList.size());
        List<String> vidLists = nodeList.stream().map(ImporSelectNodeInfo::getVid).filter(k -> StringUtils.isNotBlank(k)).collect(toList());
        List<String> codeLists = nodeList.stream().map(ImporSelectNodeInfo::getVidCode).filter(k -> StringUtils.isNotBlank(k)).collect(toList());
        for (ImporSelectNodeInfo nodeInfo : nodeList) {
            if (CollectionUtils.isNotEmpty(vidLists)&&CollectionUtils.isNotEmpty(codeLists)){
                nodeInfo.setError("同一个文件中门店ID和门店编号只能选择一个字段用于门店导入");
                errorList.add(nodeInfo);
                continue;
            }
            String info = nodeInfo.getVid()+"_"+nodeInfo.getVidCode()+"_" + nodeInfo.getVidName();
            if (msg.contains(info)) {
                nodeInfo.setError("门店重复导入");
                errorList.add(nodeInfo);
                continue;
            }
            msg.add(info);
            if (CollectionUtils.isNotEmpty(vidLists) && StringUtils.isBlank(nodeInfo.getVid())) {
                nodeInfo.setError("门店ID不可为空");
                errorList.add(nodeInfo);
                continue;
            }
            if (CollectionUtils.isNotEmpty(codeLists) && StringUtils.isBlank(nodeInfo.getVidCode())) {
                nodeInfo.setError("门店编号不可为空");
                errorList.add(nodeInfo);
                continue;
            }
            if (StringUtils.isBlank(nodeInfo.getVidName())) {
                nodeInfo.setError("门店名称不可为空");
                errorList.add(nodeInfo);
                continue;
            }
            if (CollectionUtils.isNotEmpty(vidLists)){
                try {
                    Long.valueOf(nodeInfo.getVid());
                } catch (NumberFormatException e) {
                    nodeInfo.setError("门店ID格式不正确");
                    errorList.add(nodeInfo);
                    continue;
                }
                vids.add(nodeInfo.getVid());
            }
            if (CollectionUtils.isNotEmpty(codeLists)){
                codes.add(nodeInfo.getVidCode());
            }
        }
        List<Long> vidList = vids.stream().map(Long::valueOf).distinct().collect(toList());
        List<String> codeVidList = codes.stream().distinct().collect(toList());

        Map<Long, VidInfoDto> vidMap=new HashMap<>();
        Map<String, VidInfoDto> vidCodeMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(vidLists)){
            vidMap = structureFacade.selectVidMap(vidList, reqVo.getBosId());
            nodeList = nodeList.stream().filter(node -> vids.contains(node.getVid()))
                    .filter(ValidateUtil.distinctByKey(ImporSelectNodeInfo::getVidAndName)).collect(toList());
        }
        if (CollectionUtils.isNotEmpty(codeLists)){
            GetVidListByCodeRespDTO getVidListByCodeRespDTO = structureFacade.getVidByVidCode(reqVo.getBosId(), codeVidList);
            if (Objects.nonNull(getVidListByCodeRespDTO)||CollectionUtils.isNotEmpty(getVidListByCodeRespDTO.getVidList())){
                getVidListByCodeRespDTO.getVidList().forEach(k -> {
                    vidCodeMap.put(k.getBaseInfo().getVidCode(), k);
                });
            }
            nodeList = nodeList.stream().filter(node -> codes.contains(node.getVidCode()))
                    .filter(ValidateUtil.distinctByKey(ImporSelectNodeInfo::getVidAndName)).collect(toList());
        }
        for (ImporSelectNodeInfo nodeInfo : nodeList) {
            VidInfoDto vidInfoDto =null;
            if (CollectionUtils.isNotEmpty(vidLists) && StringUtils.isNotBlank(nodeInfo.getVid())) {
                Long vid = Long.valueOf(nodeInfo.getVid());
                if (!vidMap.keySet().contains(vid)) {
                    nodeInfo.setError("门店ID不存在");
                    errorList.add(nodeInfo);
                    continue;
                }
                vidInfoDto = vidMap.get(vid);
            }
            if (CollectionUtils.isNotEmpty(codeLists) && Objects.nonNull(nodeInfo.getVidCode())) {
                vidInfoDto = vidCodeMap.get(nodeInfo.getVidCode());
                if (Objects.isNull(vidInfoDto)){
                    nodeInfo.setError("门店编号不存在");
                    errorList.add(nodeInfo);
                    continue;
                }
            }
            if(!vidInfoDto.getVidTypes().contains(VidTypeEnum.STORE.getType())) {
                String vidTypeName = vidInfoDto.getVidTypes().stream()
                        .map(type -> VidTypeEnum.enumByCode(type).getDesc())
                        .collect(Collectors.joining(","));
                nodeInfo.setError("只可导入门店类型节点,当前类型:" + vidTypeName);
                errorList.add(nodeInfo);
                continue;
            }
            if (!vidInfoDto.getBosId().equals(reqVo.getBosId())) {
                nodeInfo.setError("门店ID不在此店铺下,不可导入");
                errorList.add(nodeInfo);
                continue;
            }
            if (!vidInfoDto.getPathTree().contains(reqVo.getVid())) {
                nodeInfo.setError("此门店不归属于当前组织,不可导入");
                errorList.add(nodeInfo);
                continue;
            }
            if (!nodeInfo.getVidName().equals(vidInfoDto.getBaseInfo().getVidName())) {
                if (CollectionUtils.isNotEmpty(vidLists)){
                    nodeInfo.setError("门店ID与门店名称不匹配");
                }
                if (CollectionUtils.isNotEmpty(codeLists)){
                    nodeInfo.setError("门店编号与门店名称不匹配");
                }
                errorList.add(nodeInfo);
                continue;
            }
            nodeInfo.setVid(String.valueOf(vidInfoDto.getVid()));
            nodeInfo.setVidType(vidInfoDto.getVidTypes().get(0));
            nodeInfo.setPath(vidInfoDto.getPath());
            successList.add(nodeInfo);
        }
        resDto.setErrorList(errorList);
        resDto.setSuccessList(successList);
        resDto.setFail(errorList.size());
        resDto.setSuccessNum(successList.size());
        return resDto;
    }

    @Override
    public List<Map<String, Object>> exportNodes(ExportNodesReqVo reqVo) {
        //查询时按productInstanceId分组,结果集取扩展字段时按vidType分组
        List<ExportVidExtFieldConfigDTO> allFieldKeyList = getAllFieldKeyList(reqVo.getBosId());
        Map<Long/* productId */, Set<String>> selectFieldMap = new HashMap<>();

        allFieldKeyList.forEach(fieldKeyConfig -> {
            if (CollectionUtils.isNotEmpty(fieldKeyConfig.getExtFieldKeys())) {
                Long productId = fieldKeyConfig.getProductId();
                selectFieldMap.computeIfAbsent(productId, pid -> new HashSet<>());
                selectFieldMap.get(productId).addAll(fieldKeyConfig.getExtFieldKeys());
            }
        });

        Integer pageNum = Constants.DEFAULT_FIRST_PAGE_NUM;
        List<VidPageInfoDTO> vidInfoList = new ArrayList<>();
        Long totalCount = 0L;
        Boolean more = Boolean.FALSE;
        do {
            NodeListReqVo nodeListReqVo = reqVo.getNodeListReqVo();
            nodeListReqVo.setPageNum(pageNum++);
            nodeListReqVo.setPageSize(reqVo.getPageSize());
            VidPageRespDTO vidPageResponseVo = getVidPageResponseVo(nodeListReqVo, selectFieldMap);
            List<VidPageInfoDTO> pageInfoDTOS = vidPageResponseVo.getVidInfos();
            totalCount = vidPageResponseVo.getTotalCount();
            vidInfoList.addAll(pageInfoDTOS);
            more = vidPageResponseVo.getMore();
        } while (more && vidInfoList.size() <= exportMaxSize);
        Long bosId = reqVo.getBosId();
        List<Long> parentIds = vidInfoList.stream().map(VidPageInfoDTO::getParentVid).distinct().collect(toList());
        List<String> parentPathStrList = vidInfoList.stream().map(VidPageInfoDTO::getParentPath).distinct().collect(toList());
        List<Long> parentPathList = parentPathStrList.stream().map(k -> Arrays.asList(StringUtils.split(k, "-")))
                .flatMap(Collection::stream).filter(k -> StringUtils.isNotBlank(k) && StringUtils.isNumeric(k)).map(Long::valueOf).collect(toList());
        if (CollectionUtils.isNotEmpty(parentPathList)) {
            parentIds.addAll(parentPathList);
        }
        List<VidInfoDto> vidInfoDtos = structureFacade.selectVidInfo(parentIds);
        Map<Long, VidInfoDto> vidInfoDtoMap = vidInfoDtos.stream()
                .collect(Collectors.toMap(VidInfoDto::getVid, Function.identity()));
        RootNode rootNode = structureFacade.getRootNode(reqVo.getBosId());
        BusinessOsDto businessOsDto = structureFacade.getBosInfo(reqVo.getBosId());
        String vidTypeName = mainContentProperties.getVidTypeName(reqVo.getBosId());
        Map<Integer, List<VidPageInfoDTO>> vidTypeGroupMap = vidInfoList.stream()
                .collect(Collectors.groupingBy(vid -> vid.getVidTypes().get(0)));
        List<Integer> typeList = vidTypeGroupMap.keySet().stream().sorted(Comparator.comparing(Integer::intValue))
                .collect(toList());

        // 填充外部扩展字段的值
        allFieldKeyList.forEach(fieldKeyConfig -> {
            Long productId = fieldKeyConfig.getProductId();
            Long productInstanceId = fieldKeyConfig.getProductInstanceId();
            Integer vidType = fieldKeyConfig.getVidType();
            List<VidPageInfoDTO> fieldKeyVidList = vidTypeGroupMap.get(vidType);
            if (CollectionUtils.isEmpty(fieldKeyVidList)) {
                return;
            }

            List<Long> fieldKeyVidIdList = FunctionUtil.select(fieldKeyVidList, VidPageInfoDTO::getVid);
            List<String> withoutExtFieldKeys = fieldKeyConfig.getWithoutExtFieldKeys();
            Map<String/* withoutExtFieldKey */, String/* businessDomain */> withoutExtFieldKeyBusinessDomainMap = fieldKeyConfig.getWithoutExtFieldKeyBusinessDomainMap();
            withoutExtFieldKeys.forEach(withoutExtFieldKey -> {
                String businessDomain = withoutExtFieldKeyBusinessDomainMap.get(withoutExtFieldKey);
                Map<Long/* vid */, String/* extFieldValue */> extFieldValueMap = extFieldSpiService.getExportExtFieldPageLoop(
                        bosId, productId, productInstanceId, withoutExtFieldKey, fieldKeyVidIdList, businessDomain
                );

                fieldKeyVidList.forEach(vid -> {
                    // 补充extInfos
                    Map<Long/* productId */, NodeExtInfoDto> extInfoMap = Maps.newHashMap();
                    if (CollectionUtils.isNotEmpty(vid.getExtInfos())) {
                        extInfoMap = FunctionUtil.selectMap(vid.getExtInfos(), NodeExtInfoDto::getProductId);
                    }

                    if (extFieldValueMap != null && extFieldValueMap.containsKey(vid.getVid())) {
                        extInfoMap.computeIfAbsent(productId, pid -> {
                            NodeExtInfoDto nodeInfo = new NodeExtInfoDto();
                            nodeInfo.setProductId(productId);
                            nodeInfo.setProductInstanceId(productInstanceId);
                            nodeInfo.setExtInfoMap(Maps.newHashMap());
                            return nodeInfo;
                        });

                        extInfoMap
                                .get(productId)
                                .getExtInfoMap().put(withoutExtFieldKey, extFieldValueMap.get(vid.getVid()));
                    }

                    vid.setExtInfos(new ArrayList<>(extInfoMap.values()));
                });
            });
        });

        VidExtFieldExportTool exportTool = vidExtFieldService.instanceExportTool(bosId);
        // 填充扩展字段value
        exportAppendExt(bosId, exportTool, vidInfoList);

        //sheet结果集数据
        List<Map<String, Object>> resultList = new ArrayList<>();
        vidTypeGroupMap.forEach((vidType, vidInfos) -> {
            NodeFileReqDto templateReqVo = BeanCopyUtils.copy(reqVo, NodeFileReqDto.class);
            templateReqVo.setSelectVidType(vidType);
            Class aClass = templateExport(templateReqVo, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE);
            List<Object> exportNodeList = new ArrayList<>();
            Map<String, Object> result = new HashMap<>();
            for (VidPageInfoDTO vidInfo : vidInfos) {
                Object nodeInfo = null;
                try {
                    nodeInfo = aClass.newInstance();
                } catch (Exception e) {
                    e.printStackTrace();
                    log.info("模板对象创建失败,失败原因:{}", e);
                    ExceptionUtils.checkState(Boolean.FALSE, "导出失败");
                }
                Map<String, String> extInfoMap = new HashMap<>();
                List<NodeExtInfoDto> extInfos = vidInfo.getExtInfos();
                if (CollectionUtils.isNotEmpty(extInfos)) {
                    Optional<NodeExtInfoDto> optionalExtInfoDto = vidInfo.getExtInfos().stream().filter(dto -> dto.getProductId()
                            .equals(DEFAULT_PRODUCT_ID))
                            .findFirst();
                    if (optionalExtInfoDto.isPresent()) {
                        extInfoMap = optionalExtInfoDto.get().getExtInfoMap();
                    }
                }
                //logo拼装
                String vidLogo = null;
                if (CollectionUtils.isNotEmpty(vidInfo.getBaseInfo().getVidLogos())) {
                    vidLogo = vidInfo.getBaseInfo().getVidLogos().stream().collect(Collectors.joining(",\n"));
                }
                //图片拼装
                String vidPictures = null;
                if (StringUtils.isNotBlank(extInfoMap.get(ExtFieldKeyEnum.VID_PICTURES.getKey()))) {
                    vidPictures = JSON.parseArray(extInfoMap.get(ExtFieldKeyEnum.VID_PICTURES.getKey()), String.class)
                            .stream().collect(Collectors.joining(";\n"));
                }
                //查询组织权限
                RoleInfoDto roleInfoDto = structureService.getVidRoleInfo(reqVo.getBosId(), vidInfo.getVid());

                ContactTelDTO contactTel = null;
                if (StringUtils.isNotBlank(extInfoMap.get(ExtFieldKeyEnum.INTERNATIONAL_CONTACTTELS.getKey()))) {
                    List<ContactTelDTO> contactTelDTOS = JSON.parseArray(extInfoMap.get(ExtFieldKeyEnum.INTERNATIONAL_CONTACTTELS.getKey()), ContactTelDTO.class);
                    if (CollectionUtils.isNotEmpty(contactTelDTOS)) {
                        contactTel = contactTelDTOS.get(0);
                    }
                }
                setValue(nodeInfo, "vid", vidInfo.getVid().toString());
                setValue(nodeInfo, "vidType", vidInfo.getVidTypes().stream().map(type -> {
                    if (Objects.nonNull(rootNode) && rootNode.getVidType().equals(type)) {
                        return mainContentProperties.getVidTypeName(reqVo.getBosId());
                    } else {
                        return VidTypeEnum.enumByCode(vidType).getDesc();
                    }
                }).collect(Collectors.joining(",")));
                String parentPathStr = vidInfo.getParentPath();
                String parentPathVidName = "";
                if (StringUtils.isNotBlank(parentPathStr)) {
                    List<Long> vidParentPathList = Arrays.asList(StringUtils.split(parentPathStr, "-")).stream().filter(k -> StringUtils.isNotBlank(k) && StringUtils.isNumeric(k)).map(Long::valueOf).collect(toList());
                    if (CollectionUtils.isNotEmpty(vidParentPathList)) {
                        for (Long patentPathVid : vidParentPathList) {
                            if (Objects.equals(rootNode.getVid(), patentPathVid)) {
                                parentPathVidName = parentPathVidName + businessOsDto.getBosName();
                            } else {
                                VidInfoDto vidInfoDto = vidInfoDtoMap.get(patentPathVid);
                                parentPathVidName = parentPathVidName + "/" + vidInfoDto.getBaseInfo().getVidName();
                            }
                        }
                    }
                }

                setValue(nodeInfo, "vidParenPathName", parentPathVidName);
                setValue(nodeInfo, "vidName", vidInfo.getBaseInfo().getVidName());
                setValue(nodeInfo, "vidCode", vidInfo.getBaseInfo().getVidCode());
                setValue(nodeInfo, "parentVid", Objects.isNull(vidInfoDtoMap.get(vidInfo.getParentVid())) ? null : (vidInfoDtoMap.get(vidInfo.getParentVid()).getVid().toString()));
                setValue(nodeInfo, "parentVidName", Objects.isNull(vidInfoDtoMap.get(vidInfo.getParentVid())) ? null : vidInfoDtoMap.get(vidInfo.getParentVid()).getBaseInfo().getVidName());
                setValue(nodeInfo, "provinceName", extInfoMap.get(ExtFieldKeyEnum.ADDRESS_PROVINCE_NAME.getKey()));
                setValue(nodeInfo, "cityName", extInfoMap.get(ExtFieldKeyEnum.ADDRESS_CITY_NAME.getKey()));
                setValue(nodeInfo, "countryName", extInfoMap.get(ExtFieldKeyEnum.ADDRESS_COUNTY_NAME.getKey()));
                setValue(nodeInfo, "detailAddress", extInfoMap.get(ExtFieldKeyEnum.ADDRESS_DETAIL.getKey()));
                setValue(nodeInfo, "vidLogo", vidLogo);
                setValue(nodeInfo, "vidPictures", vidPictures);
                setValue(nodeInfo, "contactTels", Objects.isNull(contactTel) ? null : ((StringUtils.isBlank(contactTel.getZone()) ? "" :
                        (contactTel.getZone() + "-")) + (StringUtils.isBlank(contactTel.getTel()) ? "" : contactTel.getTel())));
                setValue(nodeInfo, "businessTime", StringUtils.isBlank(extInfoMap.get(ExtFieldKeyEnum.BUSINESS_TIME.getKey
                        ())) ? null : formatBusinessTime(extInfoMap.get(ExtFieldKeyEnum.BUSINESS_TIME.getKey())));
                setValue(nodeInfo, "size", extInfoMap.get(ExtFieldKeyEnum.SIZE.getKey()));
                setValue(nodeInfo, "vidPrivilege", Objects.nonNull(roleInfoDto) ? roleInfoDto.getRoleName() : null);
                setValue(nodeInfo, "vidTags", CollectionUtils.isNotEmpty(vidInfo.getTags()) ? vidInfo.getTags().stream().map(NodeTagDTO::getName).collect(Collectors.joining(";")) : null);
                Integer vidStatus = vidInfo.getBaseInfo().getVidStatus();
                String wxOnlineStatus = Objects.nonNull(extInfoMap.get(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey())) ?
                        WxOnlineStatusEnum.enumById(Integer.parseInt(extInfoMap.get(ExtFieldKeyEnum.WX_ONLINE_STATUS
                                .getKey()))).getDesc() : null;
                String isUnifyLogoEnum = IsUnifyLogoEnum.enumByCode(Objects.nonNull(extInfoMap.get(ExtFieldKeyEnum
                        .IS_UNIFY_LOGO.getKey())) ? Integer.parseInt(extInfoMap.get(ExtFieldKeyEnum.IS_UNIFY_LOGO.getKey())) : null);
                String isUnifyLogo = StringUtils.isNotBlank(isUnifyLogoEnum) ? isUnifyLogoEnum.replaceAll(VidTypeEnum.STORE
                        .getDesc(), ((Objects.nonNull(rootNode) && rootNode.getVidType().equals(vidInfo.getVidTypes().get(0))) ?
                        mainContentProperties.getVidTypeName(reqVo.getBosId()) :
                        VidTypeEnum.enumByCode(vidInfo.getVidTypes().get(0)).getDesc())) : null;
                setValue(nodeInfo, "isUnifyLogo", isUnifyLogo);
                setValue(nodeInfo, "vidStatus", VidStatusEnum.enumById(vidStatus).getDesc());
                setValue(nodeInfo, "wxOnlineStatus", wxOnlineStatus);
                if (vidInfo.getVidTypes().contains(VidTypeEnum.FLOOR.getType())) {
                    setValue(nodeInfo, "floorName", Objects.nonNull(extInfoMap.get(ExtFieldKeyEnum.FLOOR_NAME.getKey())) ? extInfoMap.get(ExtFieldKeyEnum.FLOOR_NAME.getKey()) : null);
                }
                //导出扩展字段
                if (CollectionUtils.isNotEmpty(extInfos)) {
                    for (NodeExtInfoDto extInfo : extInfos) {
                        Long productId = extInfo.getProductId();
                        if (Constants.DEFAULT_PRODUCT_ID.equals(productId)) {
                            continue;
                        }
                        for (Map.Entry<String, String> extFieldEntry : extInfo.getExtInfoMap().entrySet()) {
                            String fieldKey = extFieldEntry.getKey();
                            String fieldValue = extFieldEntry.getValue();
                            String extFieldText = exportTool.getExportExtFieldText(productId, fieldKey, fieldValue);
                            setValue(nodeInfo, fieldKey + extInfo.getProductId(), extFieldText);
                        }
                    }
                }
                exportNodeList.add(nodeInfo);
            }
            String sheetName = VidTypeEnum.enumByCode(vidType).getDesc();
            if (Objects.nonNull(rootNode) && rootNode.getVidType().equals(vidType)) {
                sheetName = vidTypeName;
            }
            result.put("sheetNo", typeList.indexOf(vidType));
            result.put("sheetName", sheetName);
            result.put("entity", aClass);
            result.put("data", exportNodeList);
            resultList.add(result);
        });
        return resultList;
    }

    /**
     * 营业时间解析
     *
     * @param businessTime
     * @return
     */
    private String formatBusinessTime(String businessTime) {
        StringBuilder sb = new StringBuilder();
        List<BusinessTimeDTO> timeDtos = JSON.parseArray(businessTime, BusinessTimeDTO.class);
        if (timeDtos.size() == 1 && org.apache.commons.collections.CollectionUtils.isEmpty(timeDtos.get(0).getBusinessDays())) {
            sb.append(timeDtos.get(0).getBusinessDesc()).append("：").append(timeDtos.get(0).getBusinessHours().stream
                    ().collect(Collectors.joining("、")));
            return sb.toString();
        }
        timeDtos.forEach(dto -> {
            sb.append(dto.getBusinessDays().stream().map(num -> BusinessTimeEnum.getTimeByMum(num)).collect(Collectors.joining
                    ("、"))).append("：").append(dto.getBusinessHours().stream().collect(Collectors.joining("、")))
                    .append(";\n");
        });
        return sb.toString();
    }

    /**
     * 节点查询,与nodeList一致
     *
     * @param reqVo
     * @param keyMap
     * @return
     */
    @SuppressWarnings("ALL")
    private VidPageRespDTO getVidPageResponseVo(NodeListReqVo
                                                        reqVo, Map<Long/* productId */, Set<String>> keyMap) {
        if (Objects.equals(Constants.DEFAULT_VID, reqVo.getParentVid())) {
            reqVo.setParentVid(null);
        }
        QueryVidPageReqDTO request = BeanCopyUtils.copy(reqVo, QueryVidPageReqDTO.class);
        request.setPage(reqVo.getPageNum());
        request.setSize(reqVo.getPageSize());
        if (Objects.nonNull(reqVo.getVidType())) {
            request.setVidTypes(Arrays.asList(reqVo.getVidType()));
        }
        if (Objects.nonNull(reqVo.getProductId())) {
            request.setProductIdList(Arrays.asList(reqVo.getProductId()));
        }
        List<VidPageExtMatchReqDTO> extMatchList = new ArrayList<>();
        if (Objects.nonNull(reqVo.getWxOnlineStatus())) {
            VidPageExtMatchReqDTO wxStatusFieldDto = new VidPageExtMatchReqDTO();
            wxStatusFieldDto.setProductId(Constants.DEFAULT_PRODUCT_ID);
            wxStatusFieldDto.setSettingKey(ExtFieldKeyEnum.WX_ONLINE_STATUS.getKey());
            wxStatusFieldDto.setSettingValues(Arrays.asList(reqVo.getWxOnlineStatus().toString()));
            extMatchList.add(wxStatusFieldDto);
        }
        if (CollectionUtils.isNotEmpty(reqVo.getExtInfoList())) {
            //查询key类型
            Map<String, Integer> fieldKeyTypeMap = structureService.getFieldKeyTypeMap(reqVo);
            for (NodeExtInfoReqVO nodeExtInfoDto : reqVo.getExtInfoList()) {
                nodeExtInfoDto.getExtInfoMap().entrySet().forEach(entry -> {
                    String value = entry.getValue();
                    if (StringUtils.isNotBlank(value)) {
                        VidPageExtMatchReqDTO extMatchReqDTO = new VidPageExtMatchReqDTO();
                        extMatchReqDTO.setProductId(nodeExtInfoDto.getProductId());
                        extMatchReqDTO.setSettingKey(entry.getKey());
                        List<String> settingValues;
                        //文本框模糊匹配
                        if (Objects.equals(FIELD_TYPE_TEXT, fieldKeyTypeMap.get(nodeExtInfoDto.getProductId() + entry.getKey()))) {
                            extMatchReqDTO.setMatch(false);
                            settingValues = Arrays.asList(value);
                        } else if (Objects.equals(FIELD_TYPE_MUL_CHOICE, fieldKeyTypeMap.get(nodeExtInfoDto.getProductId() + entry.getKey()))) {
                            settingValues = Arrays.asList(value.split(","));
                        } else {
                            settingValues = Arrays.asList(value);
                        }
                        extMatchReqDTO.setSettingValues(settingValues);
                        extMatchList.add(extMatchReqDTO);
                    }
                });
            }
        }
        structureFacade.setQueryAddress(extMatchList, BeanCopyUtils.copy(reqVo, AddressDTO.class));
        request.setExtMatchList(extMatchList);
        //扩展字段结果集
        List<VidPageExtInfoReqDTO> extInfos = new ArrayList<>();

        VidPageExtInfoReqDTO queryVidExtInfoDto = new VidPageExtInfoReqDTO();
        queryVidExtInfoDto.setProductId(Constants.DEFAULT_PRODUCT_ID);
        queryVidExtInfoDto.setProductInstanceId(productFacade.getDefaultProductInstanceId(request.getBosId()));
        queryVidExtInfoDto.setExtFields(ExtFieldKeyEnum.getAllFile());
        extInfos.add(queryVidExtInfoDto);
        keyMap.forEach((productId, keySet) -> {
            VidPageExtInfoReqDTO extInfoDto = new VidPageExtInfoReqDTO();
            extInfoDto.setProductId(productId);
            extInfoDto.setExtFields(Lists.newArrayList(keySet));
            extInfos.add(extInfoDto);
        });

        request.setExtInfos(extInfos);
        request.setResultData(Arrays.asList(NodeSearchParentNodeEnum.PARENT_NODE.getCode(), NodeSearchParentNodeEnum.TAG.getCode()));
        request.setDepthLayer(true);

        // 手动设置排序
        VidPageOrderReqDTO vidPageOrderReqDTO = new VidPageOrderReqDTO();
        vidPageOrderReqDTO.setField(VidPageOrderFiledEnum.CREATE_TIME.getField());
        vidPageOrderReqDTO.setDesc(false);
        request.setSorts(Collections.singletonList(vidPageOrderReqDTO));

        SoaResponse<VidPageRespDTO, Void> soaResponse = nodeSearchExportService.queryVidPage2(request);
        ExceptionUtils.checkState(SoaRespUtils.isSuccess(soaResponse), MpBizCode.build(soaResponse));
        ExceptionUtils.checkState(Objects.nonNull(soaResponse.getResponseVo()), MpBizCode.NOT_FOUND_NODE_LIST);
        return soaResponse.getResponseVo();
    }


    private Object buildAddress(Map<String, String> addressExtInfoMap, Object nodeInfo, VidInfoDto vidInfoDto
            , VidExtInfosDTO extInfo) {
        String address = null;
        String detailAddress = null;
        if (addressKeySet.size() == addressExtInfoMap.size()) {
            address = getKey(nodeInfo, "provinceName") +
                    getKey(nodeInfo, "cityName") +
                    getKey(nodeInfo, "countryName") +
                    getKey(nodeInfo, "detailAddress");
            detailAddress = getKey(nodeInfo, "detailAddress");
        } else if (addressExtInfoMap.size() <= 0 && vidInfoDto != null) {
            //获取原来的地址
            Long vid = vidInfoDto.getVid();
            Long bosId = vidInfoDto.getBosId();
            List<QueryVidExtInfoDto> vidExtInfos = new ArrayList<>();
            QueryVidExtInfoDto extInfoDto = new QueryVidExtInfoDto();
            extInfoDto.setProductId(Constants.DEFAULT_PRODUCT_ID);
            Long productInstanceId = productFacade.getDefaultProductInstanceId(bosId);
            extInfoDto.setProductInstanceId(productInstanceId);
            extInfoDto.setExtFields(Arrays.asList(
                    ExtFieldKeyEnum.ADDRESS_PROVINCE_NAME.getKey(),
                    ExtFieldKeyEnum.ADDRESS_CITY_NAME.getKey(),
                    ExtFieldKeyEnum.ADDRESS_COUNTY_NAME.getKey(),
                    ExtFieldKeyEnum.ADDRESS_DETAIL.getKey(),
                    ExtFieldKeyEnum.ADDRESS_LON.getKey(),
                    ExtFieldKeyEnum.ADDRESS_LAT.getKey()
            ));
            vidExtInfos.add(extInfoDto);
            Map<Long, VidInfoDto> vidInfoDtoMap = structureFacade.selectVidInfoMap(Arrays.asList(vid), bosId, vidExtInfos);
            if (vidInfoDtoMap == null || !vidInfoDtoMap.containsKey(vid)) {
                return null;
            }
            if (Objects.isNull(vidInfoDtoMap.get(vid).getExtInfos())) {
                setValue(nodeInfo, "error", ADDRESS_PARSE_FAIL_ERROR.getMessage());
                return nodeInfo;
            }
            NodeExtInfoDto nodeExtInfoDto = vidInfoDtoMap.get(vid).getExtInfos().get(0);
            Map<String, String> extInfoMap = nodeExtInfoDto.getExtInfoMap();
            address = extInfoMap.get(ExtFieldKeyEnum.ADDRESS_PROVINCE_NAME.getKey()) +
                    extInfoMap.get(ExtFieldKeyEnum.ADDRESS_CITY_NAME.getKey()) +
                    extInfoMap.get(ExtFieldKeyEnum.ADDRESS_COUNTY_NAME.getKey()) +
                    extInfoMap.get(ExtFieldKeyEnum.ADDRESS_DETAIL.getKey());
            detailAddress = extInfoMap.get(ExtFieldKeyEnum.ADDRESS_DETAIL.getKey());
            if (Strings.isBlank(address)) {
                setValue(nodeInfo, "error", ADDRESS_PARSE_FAIL_ERROR.getMessage());
                return nodeInfo;
            }
        }
        //省市区地址校验
        if (Strings.isBlank(address)) {
            return null;
        }
        SoaResponse<ParsedDivisionBO, AddressCommonError> parseAddress = parseAddress(address);
        if (!SoaRespUtils.isSuccess(parseAddress) || Objects.isNull(parseAddress.getResponseVo())) {
            setValue(nodeInfo, "error", ADDRESS_PARSE_FAIL.getMessage());
            return nodeInfo;
        }
        if (Objects.isNull(parseAddress.getResponseVo())) {
            setValue(nodeInfo, "error", ADDRESS_NOT_EXIST.getMessage());
            return nodeInfo;
        }
        AddressDTO addressDto = BeanCopyUtils.copy(parseAddress.getResponseVo(), AddressDTO.class);
        addressDto.setCountyCode(parseAddress.getResponseVo().getDistrictCode());
        addressDto.setCountyName(parseAddress.getResponseVo().getDistrictName());
        addressDto.setDetailAddress(detailAddress);
        addressDto.setLatitude(parseAddress.getResponseVo().getCoordinate().getLatitude().doubleValue());
        addressDto.setLongitude(parseAddress.getResponseVo().getCoordinate().getLongitude().doubleValue());
        extInfo.setAddressDto(addressDto);
        return null;
    }

    /**
     * distinct 自定义筛选
     *
     * @param keyExtractor function
     * @param <T>          参数
     * @return true or false
     */
    public static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = Maps.newConcurrentMap();
        return t -> Objects.isNull(seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE));
    }
}
