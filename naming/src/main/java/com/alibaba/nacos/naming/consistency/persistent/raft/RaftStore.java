/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

/**
 * @author nacos
 */
@Component
public class RaftStore {

    private Properties meta = new Properties();

    private String metaFileName = UtilsAndCommons.DATA_BASE_DIR + File.separator + "meta.properties";

    private String cacheDir = UtilsAndCommons.DATA_BASE_DIR + File.separator + "data";

    /**
     * 反序列数据，构建Datum，如果本地文件有，就序列化，如果没有
     * @param notifier
     * @param datums
     * @throws Exception
     */
    public synchronized void loadDatums(RaftCore.Notifier notifier, ConcurrentMap<String, Datum> datums) throws Exception {

        Datum datum;
        long start = System.currentTimeMillis();
        for (File cache : listCaches()) {
            //初始化命名空间内的所有服务数据
            if (cache.isDirectory() && cache.listFiles() != null) {
                for (File datumFile : cache.listFiles()) {
                    datum = readDatum(datumFile, cache.getName());
                    if (datum != null) {
                        datums.put(datum.key, datum);
                        notifier.addTask(datum.key, ApplyAction.CHANGE);
                    }
                }
                continue;
            }
            datum = readDatum(cache, StringUtils.EMPTY);
            if (datum != null) {
                datums.put(datum.key, datum);
            }
        }

        Loggers.RAFT.info("finish loading all datums, size: {} cost {} ms.", datums.size(), (System.currentTimeMillis() - start));
    }

    /**
     * #Fri Dec 27 14:53:07 CST 2019
     * meta.properties 文件内容是term=1200
     * @return
     * @throws Exception
     */
    public synchronized Properties loadMeta() throws Exception {
        File metaFile = new File(metaFileName);
        if (!metaFile.exists() && !metaFile.getParentFile().mkdirs() && !metaFile.createNewFile()) {
            throw new IllegalStateException("failed to create meta file: " + metaFile.getAbsolutePath());
        }

        try (FileInputStream inStream = new FileInputStream(metaFile)) {
            meta.load(inStream);
        }
        return meta;
    }

    public synchronized Datum load(String key) throws Exception {
        long start = System.currentTimeMillis();
        // load data
        for (File cache : listCaches()) {
            if (!cache.isFile()) {
                Loggers.RAFT.warn("warning: encountered directory in cache dir: {}", cache.getAbsolutePath());
            }

            if (!StringUtils.equals(cache.getName(), encodeFileName(key))) {
                continue;
            }

            Loggers.RAFT.info("finish loading datum, key: {} cost {} ms.",
                key, (System.currentTimeMillis() - start));
            return readDatum(cache, StringUtils.EMPTY);
        }

        return null;
    }

    public synchronized Datum readDatum(File file, String namespaceId) throws IOException {

        ByteBuffer buffer;
        FileChannel fc = null;
        try {
            fc = new FileInputStream(file).getChannel();
            buffer = ByteBuffer.allocate((int) file.length());
            fc.read(buffer);

            String json = new String(buffer.array(), StandardCharsets.UTF_8);
            if (StringUtils.isBlank(json)) {
                return null;
            }
            //00-00---000-NACOS_SWITCH_DOMAIN-000---00-00  比如 com.alibaba.nacos.naming.domains.meta.00-00---000-NACOS_SWITCH_DOMAIN-000---00-00
            /**
             * {"key":"com.alibaba.nacos.naming.domains.meta.00-00---000-NACOS_SWITCH_DOMAIN-000---00-00",
             * "timestamp":12,"value":{"adWeightMap":{},"checkTimes":3,"checksum":"","clientBeatInterval":5000,
             * "defaultCacheMillis":3000,"defaultInstanceEphemeral":true,"defaultPushCacheMillis":10000,"disableAddIP":false,
             * "distroEnabled":true,"distroServerExpiredMillis":10000,"distroThreshold":0.7,"enableAuthentication":false,"enableStandalone":true,
             * "healthCheckEnabled":true,"healthCheckWhiteList":[],"httpHealthParams":{"factor":0.85,"max":5000,"min":500},"incrementalList":[],
             * "limitedUrlMap":{},"masters":[],"mysqlHealthParams":{"factor":0.65,"max":3000,"min":2000},"name":"00-00---000-NACOS_SWITCH_DOMAIN-000---00-00",
             * "overriddenServerStatus":"UP","pushCVersion":"1.0.12","pushEnabled":true,"pushGoVersion":"0.1.0","pushJavaVersion":"0.1.0","pushPythonVersion":"0.4.3",
             * "sendBeatOnly":false,"serverStatusSynchronizationPeriodMillis":2000,"serviceStatusSynchronizationPeriodMillis":5000,
             * "tcpHealthParams":{"factor":0.75,"max":5000,"min":1000}}}
             */
            if (KeyBuilder.matchSwitchKey(file.getName())) {
                return JSON.parseObject(json, new TypeReference<Datum<SwitchDomain>>() {
                });
            }

            if (KeyBuilder.matchServiceMetaKey(file.getName())) {

                Datum<Service> serviceDatum;

                try {
                    serviceDatum = JSON.parseObject(json.replace("\\", ""), new TypeReference<Datum<Service>>() {
                    });
                } catch (Exception e) {
                    JSONObject jsonObject = JSON.parseObject(json);

                    serviceDatum = new Datum<>();
                    serviceDatum.timestamp.set(jsonObject.getLongValue("timestamp"));
                    serviceDatum.key = jsonObject.getString("key");
                    serviceDatum.value = JSON.parseObject(jsonObject.getString("value"), Service.class);
                }

                if (StringUtils.isBlank(serviceDatum.value.getGroupName())) {
                    serviceDatum.value.setGroupName(Constants.DEFAULT_GROUP);
                }
                if (!serviceDatum.value.getName().contains(Constants.SERVICE_INFO_SPLITER)) {
                    serviceDatum.value.setName(Constants.DEFAULT_GROUP
                        + Constants.SERVICE_INFO_SPLITER + serviceDatum.value.getName());
                }

                return serviceDatum;
            }

            if (KeyBuilder.matchInstanceListKey(file.getName())) {

                Datum<Instances> instancesDatum;

                try {
                    instancesDatum = JSON.parseObject(json, new TypeReference<Datum<Instances>>() {
                    });
                } catch (Exception e) {
                    JSONObject jsonObject = JSON.parseObject(json);
                    instancesDatum = new Datum<>();
                    instancesDatum.timestamp.set(jsonObject.getLongValue("timestamp"));

                    String key = jsonObject.getString("key");
                    String serviceName = KeyBuilder.getServiceName(key);
                    key = key.substring(0, key.indexOf(serviceName)) +
                        Constants.DEFAULT_GROUP + Constants.SERVICE_INFO_SPLITER + serviceName;

                    instancesDatum.key = key;
                    instancesDatum.value = new Instances();
                    instancesDatum.value.setInstanceList(JSON.parseObject(jsonObject.getString("value"),
                        new TypeReference<List<Instance>>() {
                        }));
                    if (!instancesDatum.value.getInstanceList().isEmpty()) {
                        for (Instance instance : instancesDatum.value.getInstanceList()) {
                            instance.setEphemeral(false);
                        }
                    }
                }

                return instancesDatum;
            }

            return JSON.parseObject(json, Datum.class);

        } catch (Exception e) {
            Loggers.RAFT.warn("waning: failed to deserialize key: {}", file.getName());
            throw e;
        } finally {
            if (fc != null) {
                fc.close();
            }
        }
    }

    public synchronized void write(final Datum datum) throws Exception {

        String namespaceId = KeyBuilder.getNamespace(datum.key);

        File cacheFile;

        if (StringUtils.isNotBlank(namespaceId)) {
            cacheFile = new File(cacheDir + File.separator + namespaceId + File.separator + encodeFileName(datum.key));
        } else {
            cacheFile = new File(cacheDir + File.separator + encodeFileName(datum.key));
        }

        if (!cacheFile.exists() && !cacheFile.getParentFile().mkdirs() && !cacheFile.createNewFile()) {
            MetricsMonitor.getDiskException().increment();

            throw new IllegalStateException("can not make cache file: " + cacheFile.getName());
        }

        FileChannel fc = null;
        ByteBuffer data;

        data = ByteBuffer.wrap(JSON.toJSONString(datum).getBytes(StandardCharsets.UTF_8));

        try {
            fc = new FileOutputStream(cacheFile, false).getChannel();
            fc.write(data, data.position());
            fc.force(true);
        } catch (Exception e) {
            MetricsMonitor.getDiskException().increment();
            throw e;
        } finally {
            if (fc != null) {
                fc.close();
            }
        }

        // remove old format file:
        if (StringUtils.isNoneBlank(namespaceId)) {
            if (datum.key.contains(Constants.DEFAULT_GROUP + Constants.SERVICE_INFO_SPLITER)) {
                String oldFormatKey =
                    datum.key.replace(Constants.DEFAULT_GROUP + Constants.SERVICE_INFO_SPLITER, StringUtils.EMPTY);

                cacheFile = new File(cacheDir + File.separator + namespaceId + File.separator + encodeFileName(oldFormatKey));
                if (cacheFile.exists() && !cacheFile.delete()) {
                    Loggers.RAFT.error("[RAFT-DELETE] failed to delete old format datum: {}, value: {}",
                        datum.key, datum.value);
                    throw new IllegalStateException("failed to delete old format datum: " + datum.key);
                }
            }
        }
    }

    private File[] listCaches() throws Exception {
        // 加载 nacos_home/nacos/data/naming/目录下，
        // 如果该目录不存在就创建，如果创建失败就抛异常
        File cacheDir = new File(this.cacheDir);
        if (!cacheDir.exists() && !cacheDir.mkdirs()) {
            throw new IllegalStateException("cloud not make out directory: " + cacheDir.getName());
        }

        return cacheDir.listFiles();
    }

    public void delete(Datum datum) {

        // datum key contains namespace info:
        String namespaceId = KeyBuilder.getNamespace(datum.key);

        if (StringUtils.isNotBlank(namespaceId)) {

            File cacheFile = new File(cacheDir + File.separator + namespaceId + File.separator + encodeFileName(datum.key));
            if (cacheFile.exists() && !cacheFile.delete()) {
                Loggers.RAFT.error("[RAFT-DELETE] failed to delete datum: {}, value: {}", datum.key, datum.value);
                throw new IllegalStateException("failed to delete datum: " + datum.key);
            }
        }
    }

    public void updateTerm(long term) throws Exception {
        File file = new File(metaFileName);
        if (!file.exists() && !file.getParentFile().mkdirs() && !file.createNewFile()) {
            throw new IllegalStateException("failed to create meta file");
        }

        try (FileOutputStream outStream = new FileOutputStream(file)) {
            // write meta
            meta.setProperty("term", String.valueOf(term));
            meta.store(outStream, null);
        }
    }

    private static String encodeFileName(String fileName) {
        return fileName.replace(':', '#');
    }

    private static String decodeFileName(String fileName) {
        return fileName.replace("#", ":");
    }
}
