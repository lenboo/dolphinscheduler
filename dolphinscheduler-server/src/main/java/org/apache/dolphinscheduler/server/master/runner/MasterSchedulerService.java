/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.runner;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.registry.MasterRegistryClient;
import org.apache.dolphinscheduler.service.alert.ProcessAlertManager;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * master scheduler thread
 */
@Service
public class MasterSchedulerService extends Thread {

    /**
     * logger of MasterSchedulerService
     */
    private static final Logger logger = LoggerFactory.getLogger(MasterSchedulerService.class);

    /**
     * dolphinscheduler database interface
     */
    @Autowired
    private ProcessService processService;

    /**
     * zookeeper master client
     */
    @Autowired
    private MasterRegistryClient masterRegistryClient;

    /**
     * master config
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * alert manager
     */
    @Autowired
    private ProcessAlertManager processAlertManager;

    /**
     * netty remoting client
     */
    private NettyRemotingClient nettyRemotingClient;

    /**
     * master exec service
     */
    private ThreadPoolExecutor masterExecService;

    private ConcurrentHashMap<Integer, MasterExecThread> processInstanceExecMaps ;
    private ConcurrentHashMap<Integer, MasterExecThread> eventHandlerMap = new ConcurrentHashMap();
    ListeningExecutorService listeningExecutorService;

    /**
     * constructor of MasterSchedulerService
     */
    public void init(ConcurrentHashMap<Integer, MasterExecThread> processInstanceExecMaps) {
        this.processInstanceExecMaps = processInstanceExecMaps;
        this.masterExecService = (ThreadPoolExecutor) ThreadUtils.newDaemonFixedThreadExecutor("Master-Exec-Thread", masterConfig.getMasterExecThreads());
        NettyClientConfig clientConfig = new NettyClientConfig();
        this.nettyRemotingClient = new NettyRemotingClient(clientConfig);

        ExecutorService eventService = ThreadUtils.newDaemonFixedThreadExecutor("MasterEventExecution", masterConfig.getMasterExecThreads());
        listeningExecutorService = MoreExecutors.listeningDecorator(eventService);
    }

    @Override
    public synchronized void start() {
        super.setName("MasterSchedulerService");
        super.start();
    }

    public void close() {
        masterExecService.shutdown();
        boolean terminated = false;
        try {
            terminated = masterExecService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
        if (!terminated) {
            logger.warn("masterExecService shutdown without terminated, increase await time");
        }
        nettyRemotingClient.close();
        logger.info("master schedule service stopped...");
    }

    /**
     * run of MasterSchedulerService
     */
    @Override
    public void run() {
        logger.info("master scheduler started");
        while (Stopper.isRunning()) {
            try {
                boolean runCheckFlag = OSUtils.checkResource(masterConfig.getMasterMaxCpuloadAvg(), masterConfig.getMasterReservedMemory());
                if (!runCheckFlag) {
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                    continue;
                }
                // todo 串行执行 为何还需要判断状态？
                /* if (zkMasterClient.getZkClient().getState() == CuratorFrameworkState.STARTED) {
                    scheduleProcess();
                }*/
                scheduleProcess();
                eventHandler();
            } catch (Exception e) {
                logger.error("master scheduler thread error", e);
            }
        }
    }


    private void eventHandler() {
        for (MasterExecThread masterExecThread : this.processInstanceExecMaps.values()) {

            if (masterExecThread.eventSize() == 0) {
                continue;
            }
            if (this.eventHandlerMap.contains(masterExecThread.getProcessInstance().getId())) {
                return;
            }
            eventHandlerMap.put(masterExecThread.getProcessInstance().getId(), masterExecThread);
            ListenableFuture future = this.listeningExecutorService.submit(masterExecThread);
            FutureCallback futureCallback = new FutureCallback() {
                @Override
                public void onSuccess(Object o) {
                    if (masterExecThread.workFlowFinish()) {
                        eventHandlerMap.remove(masterExecThread.getProcessInstance().getId());
                    }
                    eventHandlerMap.remove(masterExecThread.getProcessInstance().getId());
                }

                @Override
                public void onFailure(Throwable throwable) {
                }
            };
            Futures.addCallback(future, futureCallback, this.listeningExecutorService);
        }
    }


    private void scheduleProcess() throws Exception {
        try {
            masterRegistryClient.blockAcquireMutex();

            int activeCount = masterExecService.getActiveCount();
            // make sure to scan and delete command  table in one transaction
            Command command = processService.findOneCommand();
            if (command != null) {
                logger.info("find one command: id: {}, type: {}", command.getId(), command.getCommandType());

                try {

                    ProcessInstance processInstance = processService.handleCommand(logger,
                            getLocalAddress(),
                            this.masterConfig.getMasterExecThreads() - activeCount, command);
                    if (processInstance != null) {
                        logger.info("start master exec thread , split DAG ...");
                        MasterExecThread masterExecThread = new MasterExecThread(
                                processInstance
                                , processService
                                , nettyRemotingClient
                                , processAlertManager
                                , masterConfig);

                        this.processInstanceExecMaps.put(processInstance.getId(), masterExecThread);
                        masterExecService.execute(masterExecThread);
                    }
                } catch (Exception e) {
                    logger.error("scan command error ", e);
                    processService.moveToErrorCommand(command, e.toString());
                }
            } else {
                //indicate that no command ,sleep for 1s
                Thread.sleep(Constants.SLEEP_TIME_MILLIS);
            }
        } finally {
            masterRegistryClient.releaseLock();
        }
    }

    private String getLocalAddress() {
        return NetUtils.getAddr(masterConfig.getListenPort());
    }
}
