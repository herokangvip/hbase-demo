package com.heroking.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @author k
 * @version 1.0
 * @date 2019/12/3 22:43
 */
public class HBaseFactory {

    public static void main(String[] args) throws Exception {
        Connection connection = HBaseFactory.getConnection();
        System.out.println("===");
    }

    private static Configuration conf = null;
    private static volatile Connection conn = null;

    /**
     * 获取全局唯一的Configuration实例
     *
     * @return
     */
    private static synchronized Configuration getConfiguration() {
        if (conf == null) {
            // 此处从配置文件读取配置信息，配置文件在classpath下的hbase-site.xml。
            conf = HBaseConfiguration.create();
        }
        return conf;
    }

    /**
     * 获取全局唯一的Connection实例
     * Connection对象自带连接池，请使用单例模式获取连接。
     *
     * @return Connection
     */
    public static Connection getConnection()
            throws Exception {
        if (conn == null || conn.isClosed() || conn.isAborted()) {
            synchronized (HBaseFactory.class) {
                if (conn == null || conn.isClosed() || conn.isAborted()) {
                    /*
                     * * 创建一个Connection
                     */
                    //第一种方式：通过配置文件
                    Configuration configuration = getConfiguration();
                    //第二种方式：代码中指定
                    //Configuration configuration = new Configuration();
                    //configuration.set("hbase.zookeeper.property.clientPort", "2181");
                    //configuration.set("hbase.zookeeper.quorum", "server01");
                    conn = ConnectionFactory.createConnection(configuration);
                }
            }
        }
        return conn;
    }
}
