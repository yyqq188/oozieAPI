package com.pactera.oozieAPI.json2sql.utils;

import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConfig {

	private String zookeeperQuorum="10.114.10.92,10.114.10.93,10.114.10.94";

	private String clientPort="2181";

	private int rpcTimeOut=20000;

	private int operationTimeOut=30000;

	private int scannerTimeoutPeriod=200000;


	private org.apache.hadoop.conf.Configuration conf = null;

	public org.apache.hadoop.conf.Configuration getConfiguration(){
		conf = HBaseConfiguration.create();
		conf.set( "hbase.zookeeper.quorum", zookeeperQuorum);
		if(clientPort!=null) {
			conf.set("hbase.zookeeper.property.clientPort", clientPort);
		}
		conf.setInt( "hbase.rpc.timeout", rpcTimeOut );
		conf.setInt( "hbase.client.operation.timeout", operationTimeOut );
		conf.setInt( "hbase.client.scanner.timeout.period", scannerTimeoutPeriod );
		return  conf;
	}

	public String getZookeeperQuorum() {
		return zookeeperQuorum;
	}

	public void setZookeeperQuorum(String zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}

	public String getClientPort() {
		return clientPort;
	}

	public void setClientPort(String clientPort) {
		this.clientPort = clientPort;
	}

	public int getRpcTimeOut() {
		return rpcTimeOut;
	}

	public void setRpcTimeOut(int rpcTimeOut) {
		this.rpcTimeOut = rpcTimeOut;
	}

	public int getOperationTimeOut() {
		return operationTimeOut;
	}

	public void setOperationTimeOut(int operationTimeOut) {
		this.operationTimeOut = operationTimeOut;
	}

	public int getScannerTimeoutPeriod() {
		return scannerTimeoutPeriod;
	}

	public void setScannerTimeoutPeriod(int scannerTimeoutPeriod) {
		this.scannerTimeoutPeriod = scannerTimeoutPeriod;
	}
}
