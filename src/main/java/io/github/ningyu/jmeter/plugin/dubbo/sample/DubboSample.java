/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.ningyu.jmeter.plugin.dubbo.sample;

import io.github.ningyu.jmeter.plugin.dubbo.exception.BusinessException;
import io.github.ningyu.jmeter.plugin.util.ClassUtils;
import io.github.ningyu.jmeter.plugin.util.Constants;
import io.github.ningyu.jmeter.plugin.util.ErrorCode;
import io.github.ningyu.jmeter.plugin.util.JsonUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConfigCenterConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * DubboSample
 */
public class DubboSample extends AbstractSampler implements Interruptible {

    private static final Logger log = LoggingManager.getLoggerForClass();
    private static final long serialVersionUID = -6794913295411458705L;


    public static ApplicationConfig application = new ApplicationConfig("DubboSample");

    private static Map<DubboTuple, GenericService> referenceServiceMap = new ConcurrentHashMap<>();
    private ReentrantLock lock = new ReentrantLock();

    @SuppressWarnings("deprecation")
	@Override
    public SampleResult sample(Entry entry) {
        SampleResult res = new SampleResult();
        res.setSampleLabel(getName());
        //构造请求数据
        DubboTuple tuple = getDubboTuple();

        res.setSamplerData(getSampleData(tuple));
        //调用dubbo
        res.setResponseData(JsonUtils.toJson(callDubbo(res, tuple)), StandardCharsets.UTF_8.name());
        //构造响应数据
        res.setDataType(SampleResult.TEXT);
        return res;
    }


    @Data
    @EqualsAndHashCode
    class DubboTuple {
        private String registryProtocol;
        private String registryGroup;
        private String registryTimeout;
        private String address;
        private String rpcProtocol;
        private String timeout;
        private String version;
        private String retries;
        private String cluster;
        private String group;
        private String connections;
        private String loadbalance;
        private String async;
        private String _interface;
        private String method;
        private List<MethodArgument> methodArgs;
        private List<MethodArgument> attachmentArgs;
    }
    private DubboTuple getDubboTuple() {
        DubboTuple tuple = new DubboTuple();
        tuple.registryProtocol = Constants.getRegistryProtocol(this);
        tuple.registryGroup = Constants.getRegistryGroup(this);
        tuple.registryTimeout = Constants.getRegistryTimeout(this);
        tuple.address = Constants.getAddress(this);
        tuple.rpcProtocol = Constants.getRpcProtocol(this);
        tuple.timeout = Constants.getTimeout(this);
        tuple.version = Constants.getVersion(this);
        tuple.retries = Constants.getRetries(this);
        tuple.cluster = Constants.getCluster(this);
        tuple.group = Constants.getGroup(this);
        tuple.connections = Constants.getConnections(this);
        tuple.loadbalance = Constants.getLoadbalance(this);
        tuple.async = Constants.getAsync(this);
        tuple._interface = Constants.getInterface(this);
        tuple.method = Constants.getMethod(this);
        tuple.methodArgs = Constants.getMethodArgs(this);
        tuple.attachmentArgs = Constants.getAttachmentArgs(this);
        return tuple;
    }
    /**
     * Construct request data
     */
    private String getSampleData(DubboTuple tuple) {
        log.info("sample中的实例id"+this.toString()+",element名称"+this.getName());
    	StringBuilder sb = new StringBuilder();
        sb.append("Registry Protocol: ").append(tuple.registryProtocol).append("\n");
        sb.append("Address: ").append(tuple.address).append("\n");
        sb.append("RPC Protocol: ").append(tuple.rpcProtocol).append("\n");
        sb.append("Timeout: ").append(tuple.timeout).append("\n");
        sb.append("Version: ").append(tuple.version).append("\n");
        sb.append("Retries: ").append(tuple.retries).append("\n");
        sb.append("Cluster: ").append(tuple.cluster).append("\n");
        sb.append("Group: ").append(tuple.group).append("\n");
        sb.append("Connections: ").append(tuple.connections).append("\n");
        sb.append("LoadBalance: ").append(tuple.loadbalance).append("\n");
        sb.append("Async: ").append(tuple.async).append("\n");
        sb.append("Interface: ").append(tuple._interface).append("\n");
        sb.append("Method: ").append(tuple.method).append("\n");
        sb.append("Method Args: ").append(tuple.methodArgs.toString());
        sb.append("Attachment Args: ").append(tuple.attachmentArgs.toString());
        return sb.toString();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object callDubbo(SampleResult res, DubboTuple tuple) {

        try {

            GenericService genericService = getGenericService(tuple);

            List<MethodArgument> args = Constants.getMethodArgs(this);
            List<String> paramterTypeList = new ArrayList<>();;
            List<Object> parameterValuesList = new ArrayList<>();;
            for(MethodArgument arg : args) {
                ClassUtils.parseParameter(paramterTypeList, parameterValuesList, arg);
            }
            String[] parameterTypes = paramterTypeList.toArray(new String[0]);
            Object[] parameterValues = parameterValuesList.toArray(new Object[0]);

            List<MethodArgument> attachmentArgs = Constants.getAttachmentArgs(this);
            if (attachmentArgs != null && !attachmentArgs.isEmpty()) {
                RpcContext.getContext().setAttachments(attachmentArgs.stream().collect(Collectors.toMap(MethodArgument::getParamType, MethodArgument::getParamValue)));
            }

            String methodName = tuple.method;

            res.sampleStart();
            Object result = null;
			try {
				result = genericService.$invoke(methodName, parameterTypes, parameterValues);
                res.setResponseOK();
			} catch (Exception e) {
				log.error("Exception：", e);
                if (e instanceof RpcException) {
                    RpcException rpcException = (RpcException) e;
                    setResponseError(res, String.valueOf(rpcException.getCode()), rpcException.getMessage());
                } else {
                    setResponseError(res, ErrorCode.UNKNOWN_EXCEPTION);
                }
				result = e;
			}
            res.sampleEnd();
            return result;
        } catch (BusinessException e) {
            setResponseError(res, e.getErrorCode());
            return e.getErrorCode().getMessage();
        } catch (Exception e) {
            log.error("UnknownException：", e);
            setResponseError(res, ErrorCode.UNKNOWN_EXCEPTION);
            return e;
        } finally {
        	//TODO 不能在sample结束时destroy
//            if (registry != null) {
//                registry.destroyAll();
//            }
//            reference.destroy();
        }
    }

    private GenericService getGenericService(DubboTuple tuple) throws BusinessException {
        GenericService genericService = referenceServiceMap.get(tuple);
        if (genericService == null) {
            lock.lock();
            try {
                // This instance is heavy, encapsulating the connection to the registry and the connection to the provider,
                // so please cache yourself, otherwise memory and connection leaks may occur.
                ReferenceConfig<GenericService> reference = new ReferenceConfig<>();
                // set application
                reference.setApplication(application);
                setConfigRegister(reference, tuple);
                /** registry center */
                String address = Constants.getAddress(this);
                if (StringUtils.isBlank(address)) {
                    throw new BusinessException(ErrorCode.MISS_ADDRESS);
                }
                String interfaceName = Constants.getInterface(this);
                if (StringUtils.isBlank(interfaceName)) {
                    throw new BusinessException(ErrorCode.MISS_INTERFACE);
                }
                reference.setInterface(interfaceName);
                // set retries
                Integer retries = null;
                try {
                    if (!StringUtils.isBlank(Constants.getRetries(this))) {
                        retries = Integer.valueOf(Constants.getRetries(this));
                    }
                } catch (NumberFormatException e) {
                    throw new BusinessException(ErrorCode.RETRIES_ERROR);
                }
                if (retries != null) {
                    reference.setRetries(retries);
                }

                // set cluster
                String cluster = Constants.getCluster(this);
                if (!StringUtils.isBlank(cluster)) {
                    reference.setCluster(Constants.getCluster(this));
                }

                // set version
                String version = Constants.getVersion(this);
                if (!StringUtils.isBlank(version)) {
                    reference.setVersion(version);
                }

                // set timeout
                Integer timeout = null;
                try {
                    if (!StringUtils.isBlank(Constants.getTimeout(this))) {
                        timeout = Integer.valueOf(Constants.getTimeout(this));
                    }
                } catch (NumberFormatException e) {
                    throw new BusinessException(ErrorCode.TIMEOUT_ERROR);
                }
                if (timeout != null) {
                    reference.setTimeout(timeout);
                }

                // set group
                String group = Constants.getGroup(this);
                if (!StringUtils.isBlank(group)) {
                    reference.setGroup(group);
                }

                // set connections
                Integer connections = null;
                try {
                    if (!StringUtils.isBlank(Constants.getConnections(this))) {
                        connections = Integer.valueOf(Constants.getConnections(this));
                    }
                } catch (NumberFormatException e) {
                    throw new BusinessException(ErrorCode.CONNECTIONS_ERROR);
                }
                if (connections != null) {
                    reference.setConnections(connections);
                }

                // set loadBalance
                String loadBalance = Constants.getLoadbalance(this);
                if (!StringUtils.isBlank(loadBalance)) {
                    reference.setLoadbalance(loadBalance);
                }

                // set async
                String async = Constants.getAsync(this);
                if (!StringUtils.isBlank(async)) {
                    reference.setAsync(Constants.ASYNC.equals(async));
                }

                // set generic
                reference.setGeneric(true);

                String methodName = Constants.getMethod(this);
                if (StringUtils.isBlank(methodName)) {
                    throw new BusinessException(ErrorCode.MISS_METHOD);
                }

                // The registry's address is to generate the ReferenceConfigCache key
                /*ReferenceConfigCache cache = ReferenceConfigCache.getCache(Constants.getAddress(this), new ReferenceConfigCache.KeyGenerator() {
                    @Override
                    public String generateKey(org.apache.dubbo.config.ReferenceConfig<?> referenceConfig) {
                        return referenceConfig.toString();
                    }
                });

                genericService = (GenericService) cache.get(reference);*/
                genericService = reference.get();
                if (genericService == null) {
                    throw new BusinessException(ErrorCode.GENERIC_SERVICE_IS_NULL);
                }
                referenceServiceMap.put(tuple, genericService);

            } catch (Exception e) {
                log.error("failed to get generic service", e);
            } finally {
                lock.unlock();
            }

            return referenceServiceMap.get(tuple);
        }

        return genericService;

    }

    private void setConfigRegister(ReferenceConfig reference, DubboTuple tuple) throws BusinessException {
        RegistryConfig registry = null;
        String address = tuple.address;
        String rpcProtocol = tuple.rpcProtocol.replaceAll("://", "");
        String protocol = tuple.registryProtocol;
        String _interface = tuple._interface;
        String registryGroup = tuple.registryGroup;
        Integer registryTimeout = null;
        try {
            String registryTimeoutStr = tuple.registryTimeout;
            if (StringUtils.isNotBlank(registryTimeoutStr)) {
                registryTimeout = Integer.valueOf(registryTimeoutStr);
            }
        } catch (NumberFormatException e) {
            throw new BusinessException(ErrorCode.TIMEOUT_ERROR);
        }
        if (StringUtils.isBlank(protocol) || Constants.REGISTRY_NONE.equals(protocol)) {
            //direct connection
            StringBuffer sb = new StringBuffer();
            sb.append(tuple.rpcProtocol)
                    .append(address)
                    .append("/").append(_interface);
            log.debug("rpc invoker url : " + sb.toString());
            reference.setUrl(sb.toString());
        } else if(Constants.REGISTRY_SIMPLE.equals(protocol)){
            registry = new RegistryConfig();
            registry.setAddress(address);
            reference.setProtocol(rpcProtocol);
            reference.setRegistry(registry);
        } else {
            registry = new RegistryConfig();
            registry.setProtocol(protocol);
            registry.setGroup(registryGroup);
            registry.setAddress(address);
            if (registryTimeout != null) {
                registry.setTimeout(registryTimeout);
            }
            reference.setProtocol(rpcProtocol);
            reference.setRegistry(registry);
        }
        /** config center */
        try {
            String configCenterProtocol = Constants.getConfigCenterProtocol(this);
            if (!StringUtils.isBlank(configCenterProtocol)) {
                String configCenterGroup = Constants.getConfigCenterGroup(this);
                String configCenterNamespace = Constants.getConfigCenterNamespace(this);
                String configCenterAddress = Constants.getConfigCenterAddress(this);
                if (StringUtils.isBlank(configCenterAddress)) {
                    throw new BusinessException(ErrorCode.MISS_ADDRESS);
                }
                Long configCenterTimeout = null;
                try {
                    if (!StringUtils.isBlank(Constants.getConfigCenterTimeout(this))) {
                        configCenterTimeout = Long.valueOf(Constants.getConfigCenterTimeout(this));
                    }
                } catch (NumberFormatException e) {
                    throw new BusinessException(ErrorCode.TIMEOUT_ERROR);
                }
                ConfigCenterConfig configCenter = new ConfigCenterConfig();
                configCenter.setProtocol(configCenterProtocol);
                configCenter.setGroup(configCenterGroup);
                configCenter.setNamespace(configCenterNamespace);
                configCenter.setAddress(configCenterAddress);
                if (configCenterTimeout != null) {
                    configCenter.setTimeout(configCenterTimeout);
                }
                reference.setConfigCenter(configCenter);
            }
        } catch (IllegalStateException e) {
            /** Duplicate Config */
            throw new BusinessException(ErrorCode.DUPLICATE_CONFIGCENTERCONFIG);
        }
    }

    public void setResponseError(SampleResult res, ErrorCode errorCode) {
        setResponseError(res, errorCode.getCode(), errorCode.getMessage());
    }
    public void setResponseError(SampleResult res, String code, String message) {
        res.setSuccessful(false);
        res.setResponseCode(code);
        res.setResponseMessage(message);
    }

    @Override
    public boolean interrupt() {
        Thread.currentThread().interrupt();
        return true;
    }
}
