package org.apache.yarn1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class YarnApp implements Watcher {

    protected Configuration conf;
    private ZooKeeper zk;

    public YarnApp() {
        conf = new YarnConfiguration();
    }

    protected String getDefaultZooKeeperConnect() {
        return conf.get(YarnConfiguration.RM_ZK_ADDRESS, "localhost");
    }

    protected ZooKeeper getDefaultZooKeeper() throws IOException {
        if (zk == null) {
            YarnConfiguration yarnConfiguration = new YarnConfiguration();
            String zkConnet = yarnConfiguration.get(YarnConfiguration.RM_ZK_ADDRESS, "localhost");
            zk = new ZooKeeper(zkConnet, 6000, this);
        }
        return zk;
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub
    }
}
