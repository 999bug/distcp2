package org.apache.hadoop.tools.appendnf.aio;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.appendnf.DistCpUtils;
import org.apache.hadoop.tools.appendnf.RetriableFileCopyCommand;
import org.apache.hadoop.tools.appendnf.consts.I2BBAction;
import org.apache.hadoop.tools.appendnf.consts.I2Const;
import org.apache.hadoop.tools.appendnf.consts.I2bbFileAction;
import org.apache.hadoop.tools.appendnf.consts.TransDTO;
import org.apache.hadoop.tools.appendnf.dao.I2BBFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.Future;

/**
 * @Author: LiSY
 * @Date: 2021/10/22 15:15
 */
public class HjobRequestHandler {

    private HjobRequestHandler() {
    }

    I2bbFileAction action = I2bbFileAction.OVERWRITE;


    private static final Logger logger = LoggerFactory.getLogger(HjobRequestHandler.class);

    public static TransDTO received(AsynchronousSocketChannel socketChannel, String fileName,
                                    CopyListingFileStatus sourceFileStatus,
                                    Mapper.Context context) {
        try {
            logger.info("fileName " + fileName);
            // 发送数据
            ByteBuffer byteBuffer = ByteBuffer.wrap(fileName.getBytes());
            Future<Integer> write = socketChannel.write(byteBuffer);
            while (!write.isDone()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


            // 接收数据
            ByteBuffer allocate = ByteBuffer.allocate(8192);
            Future<Integer> read = socketChannel.read(allocate);
            Integer totalSize = read.get();
            String checkSkip = new String(allocate.array(), 0, 3);
            logger.info("checkSkip " + checkSkip);
            String fileStatusObject = new String(allocate.array(), 3, totalSize);
            logger.info("fileStatusObject " + fileStatusObject);

            I2bbFileAction action;
            I2BBFileStatus status = JSON.parseObject(fileStatusObject, I2BBFileStatus.class);
            logger.info("status " + status);
            FileSystem sourceFs = sourceFileStatus.getPath().getFileSystem(context.getConfiguration());
            switch (checkSkip) {
                case I2BBAction.CHECK_EXIT:
                    logger.info("存在此文件 {}", fileName);
                    logger.info(status + "");
                    action = checkUpdate(sourceFs, sourceFileStatus, status);
                    break;
                case I2BBAction.CHECK_NO_EXIT:
                    logger.info("不存在此文件 {}", fileName);
                    action = I2bbFileAction.OVERWRITE;
                    break;
                default:
                    throw new RuntimeException("error message");
            }
            logger.info("action " + action);
            return new TransDTO(status, action);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * @return I2bbFileAction
     * @Description 判断是否是 append
     * @Author LiSY
     * @Date 2021/10/22 17:15
     * @param: sourceFS
     * @param: source
     */
    private static I2bbFileAction checkUpdate(FileSystem sourceFS,
                                              CopyListingFileStatus source, I2BBFileStatus status) {
        try {
            if (canSkip(sourceFS, source, status)) {
                return I2bbFileAction.SKIP;
            } else {
                long targetLength = status.getTrunkSize();
                if (source.getLen() > status.getTrunkSize()) {
                    FileChecksum sourceChecksum = sourceFS.getFileChecksum(source.getPath(), targetLength);
                    String sourceChe = sourceChecksum.toString().split(":")[1];
                    if (sourceChe.equals(status.getCrc35())) {
                        return I2bbFileAction.APPEND;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return I2bbFileAction.OVERWRITE;
    }

    // 判断是否需要跳过此文件
    private static boolean canSkip(FileSystem sourceFS, CopyListingFileStatus source, I2BBFileStatus status) throws IOException {
        boolean sameLength = status.getTrunkSize() == source.getLen();
        if (sameLength) {
            return DistCpUtils.checksumsAreEqual(sourceFS, source.getPath(), status);
        } else {
            return false;
        }
    }


}
