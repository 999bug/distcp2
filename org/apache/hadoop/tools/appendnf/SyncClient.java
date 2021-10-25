package org.apache.hadoop.tools.appendnf;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.appendnf.aio.HjobRequestHandler;
import org.apache.hadoop.tools.appendnf.consts.I2Const;
import org.apache.hadoop.tools.appendnf.consts.TransDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.Future;

/**
 * @Author: LiSY
 * @Date: 2021/10/22 9:23
 * hadoop 集群中
 */
public class SyncClient {
    private static final Logger logger = LoggerFactory.getLogger(SyncClient.class);
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", I2Const.GET_FILE_STATUS);

    private static SyncClient instance = null;

    private SyncClient() {
    }

    public static SyncClient getInstance() {
        if (instance == null) {
            synchronized (SyncClient.class) {
                if (instance == null) {
                    instance = new SyncClient();
                }
            }
        }
        return instance;
    }

    public TransDTO syncClient(CopyListingFileStatus sourceFileStatus, Mapper.Context context) throws IOException, InterruptedException {


        String fileName = sourceFileStatus.getPath().getName();
        AsynchronousSocketChannel socketChannel = AsynchronousSocketChannel.open();
        Future<Void> connect = socketChannel.connect(ADDRESS);
        while (!connect.isDone()) {
            Thread.sleep(10);
        }
        logger.info("与服务器 {} 建立连接", socketChannel.getRemoteAddress());
       return HjobRequestHandler.received(socketChannel, fileName, sourceFileStatus, context);
    }
}
