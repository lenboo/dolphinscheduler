package org.apache.dolphinscheduler.common.zk.monitor;

import org.apache.dolphinscheduler.common.zk.operation.ZKNodeOperationImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  monitor server
 */
public class MonitorServer {

    private static final Logger logger = LoggerFactory.getLogger(MonitorServer.class);

    private static Integer ARGS_LENGTH = 4;

    public static void main(String[] args) throws Exception{

        if (args.length != ARGS_LENGTH){
            logger.error("Usage: <masterPath> <workerPath> <port> <installPath>");
            return;
        }

        String masterPath = args[0];
        String workerPath = args[1];
        Integer port = Integer.parseInt(args[2]);
        String installPath = args[3];

        Monitor monitorServer = new ZKMonitorImpl(new ZKNodeOperationImpl());
        monitorServer.monitor(masterPath,workerPath,port,installPath);
    }
}
