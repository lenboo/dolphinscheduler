package org.apache.dolphinscheduler.common.zk.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * remove zk node
 */
public class RemoveZKNode {

    private static final Logger logger = LoggerFactory.getLogger(RemoveZKNode.class);

    private static Integer ARGS_LENGTH = 1;

    public static void main(String[] args) {

        if (args.length != ARGS_LENGTH){
            logger.error("Usage: <node>");
            return;
        }

        NodeOperation nodeOperation = new ZKNodeOperationImpl();
        nodeOperation.removeNode(args[0]);
    }
}
