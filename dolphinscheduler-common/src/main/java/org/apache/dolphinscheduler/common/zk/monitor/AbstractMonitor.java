package org.apache.dolphinscheduler.common.zk.monitor;

import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * abstract server monitor and auto restart server
 */
public abstract class AbstractMonitor implements Monitor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMonitor.class);

    /**
     * monitor server and restart
     */
    @Override
    public void monitor(String masterPath,String workerPath,Integer port,String installPath) {
        try {
            restartServer(masterPath,port,installPath);
            restartServer(workerPath,port,installPath);
        }catch (Exception e){
            logger.error("server start up error",e);
        }
    }

    private void restartServer(String path,Integer port,String installPath) throws Exception{

        String type = path.split("/")[2];
        String serverName = null;
        String nodes = null;
        if ("masters".equals(type)){
            serverName = "master-server";
            nodes = PropertyUtils.getString("masters");
        }else if ("workers".equals(type)){
            serverName = "worker-server";
            nodes = PropertyUtils.getString("workers");
        }

        Map<String, String> activeNodeMap = getActiveNodesByPath(path);

        Set<String> needRestartServer = getNeedRestartServer(getRunConfigServer(nodes),
                activeNodeMap.keySet());

        for (String node : needRestartServer){
            // os.system('ssh -p ' + ssh_port + ' ' + self.get_ip_by_hostname(master) + ' sh ' + install_path + '/bin/dolphinscheduler-daemon.sh start master-server')
            String runCmd = "ssh -p " + port + " " +  node + " sh "  + installPath + "/bin/dolphinscheduler-daemon.sh start " + serverName;
            Runtime.getRuntime().exec(runCmd);
        }
    }

    /**
     * get need restart server
     * @param deployedNodes  deployedNodes
     * @param activeNodes activeNodes
     * @return need restart server
     */
    private Set<String> getNeedRestartServer(Set<String> deployedNodes,Set<String> activeNodes){
        if (CollectionUtils.isEmpty(activeNodes)){
            return deployedNodes;
        }

        Set<String> result = new HashSet<>();

        result.addAll(deployedNodes);
        result.removeAll(activeNodes);

        return result;
    }

    /**
     * run config masters/workers
     * @return master set/worker set
     */
    private Set<String> getRunConfigServer(String nodes){
        Set<String> nodeSet = new HashSet();


        if (StringUtils.isEmpty(nodes)){
            return null;
        }

        String[] nodeArr = nodes.split(",");

        for (String node : nodeArr){
            nodeSet.add(node);
        }

        return nodeSet;
    }

    /**
     * get active nodes by path
     * @param path path
     * @return active nodes
     */
    protected abstract Map<String,String> getActiveNodesByPath(String path);
}
