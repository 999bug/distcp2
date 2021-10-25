package org.apache.hadoop.tools.appendnf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.appendnf.aio.HjobRequestHandler;
import org.apache.hadoop.tools.appendnf.consts.I2Const;
import org.apache.hadoop.tools.appendnf.consts.I2bbFileAction;
import org.apache.hadoop.tools.appendnf.dao.I2BBFileStatus;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.apache.hadoop.tools.util.ThrottledInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.text.DecimalFormat;
import java.util.concurrent.Future;

/**
 * @Author: LiSY
 * @Date: 2021/10/25 9:32
 */
public class RetriableFileCopyCommand {
    private static final Logger logger = LoggerFactory.getLogger(RetriableFileCopyCommand.class);

    private I2bbFileAction action;
    private String description;

    public RetriableFileCopyCommand(String description, I2bbFileAction action) {
        this.description = description;
        this.action = action;

    }

    public long execute(CopyListingFileStatus source,
                        I2BBFileStatus targetFs, I2bbFileAction action,
                        Mapper.Context context,
                        AsynchronousSocketChannel socketChannel) {
        final boolean toAppend = action == I2bbFileAction.APPEND;
        logger.info("append {}", toAppend);
        String relPath = targetFs.getRelPath();
        logger.info("target file path {}", relPath);

        long bytesRead = 0;

        final Configuration configuration = context.getConfiguration();
        FileSystem sourceFs;

        try {
            sourceFs = source.getPath().getFileSystem(configuration);
            long offset = (action == I2bbFileAction.APPEND) ?
                    targetFs.getOffset() : source.getChunkOffset();
logger.warn("offset" + offset);
            // default
            int copyBufferSize = 0;
            bytesRead = copyToFile(source, offset, context, copyBufferSize, socketChannel);

            if (!source.isSplit()) {
                // 检查传输文件的大小是否一致
                compareFileLengths(source, configuration, bytesRead
                        + offset);
            }

            // 在这一点上，src和dest的长度是相同的。 如果长度==0，我们就跳过校验。
            if (bytesRead != 0) {
                if (!source.isSplit()) {
                    compareCheckSums(sourceFs, source.getPath(), targetFs);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bytesRead;
    }

    private void compareCheckSums(FileSystem sourceFS, Path source, I2BBFileStatus targetFs)
            throws IOException {
        if (!DistCpUtils.checksumsAreEqual(sourceFS, source, targetFs)) {
            StringBuilder errorMessage = new StringBuilder("Check-sum mismatch between ")
                    .append(source).append(" and ").append("target").append(".");
            throw new IOException(errorMessage.toString());
        }
    }

    private void compareFileLengths(CopyListingFileStatus source,
                                    Configuration configuration, long targetLen)
            throws IOException {
        final Path sourcePath = source.getPath();
        FileSystem fs = sourcePath.getFileSystem(configuration);
        long srcLen = fs.getFileStatus(sourcePath).getLen();
        if (srcLen != targetLen) {
            throw new IOException("Mismatch in length of source:" + sourcePath + " (" + srcLen +
                    ") and target:" + " (" + targetLen + ")");
        }
    }

    private void sendMsg(byte[] bytes, int offset, int len, AsynchronousSocketChannel socketChannel) {
        ByteBuffer wrap = ByteBuffer.wrap(bytes, offset, len);
        Future<Integer> write = socketChannel.write(wrap);
        while (!write.isDone()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("服务器数据发送完毕");
    }

    private long copyToFile(CopyListingFileStatus source2,
                            long sourceOffset, Mapper.Context context,
                            int copyBufferSize, AsynchronousSocketChannel socketChanne) throws IOException {

        Path source = source2.getPath();
        byte[] buf = new byte[copyBufferSize];
        long totalBytesRead = 0;
        long chunkLength = source2.getChunkLength();
        boolean finished = false;
        FSDataInputStream inStream = getInputStream(source, context.getConfiguration());
        int totalSum =0;
        int offset = 0;

        try {
            while (totalSum <= chunkLength) {

                inStream = getInputStream(source, context.getConfiguration());
                for (int bytesRead = inStream.read(buf); bytesRead >= 0; bytesRead = inStream.read(buf)) {
                    logger.warn("bytesRead " + bytesRead);
                    sendMsg(buf, offset, bytesRead, socketChanne);
                    offset += copyBufferSize;
                    totalSum += copyBufferSize;
                }
            }
            int bytesRead = readBytes(inStream, buf, sourceOffset);
            logger.warn("bytesRead " + bytesRead);// 30723 //最大为65536
            logger.warn("chunkLength " + chunkLength);// 文件总大小
            while (bytesRead >= 0) {
                if (chunkLength > 0 && (totalBytesRead + bytesRead) >= chunkLength) {
                    bytesRead = (int) (chunkLength - totalBytesRead);
                    finished = true;
                }
                totalBytesRead += bytesRead;
                // append
                if (action == I2bbFileAction.APPEND) {
                    sourceOffset += bytesRead;
                }
                logger.warn("我执行了几次？？");
                sendMsg(buf, 0, bytesRead, socketChanne);
                // 打印 map 的进度
                updateContextStatus(totalBytesRead, context, source2);
                if (finished) {
                    break;
                }
                bytesRead = readBytes(inStream, buf, sourceOffset);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return totalBytesRead;
    }

    private static FSDataInputStream getInputStream(Path path, Configuration conf) throws IOException {
        try {
            return path.getFileSystem(conf).open(path);
        } catch (IOException e) {
            throw new org.apache.hadoop.tools.mapred.RetriableFileCopyCommand.CopyReadException(e);
        }
    }


    private static int readBytes(FSDataInputStream inStream, byte[] bytes, long position) {
        try {
            if (position == 0) {
                return inStream.read(bytes);
            } else {
                return inStream.read(position, bytes, 0, bytes.length);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateContextStatus(long totalBytesRead, Mapper.Context context,
                                     CopyListingFileStatus source2) {
        StringBuilder message = new StringBuilder(getFormatter()
                .format(totalBytesRead * 100.0f / source2.getLen()));
        message.append("% ")
                .append(description).append(" [")
                .append(getStringDescriptionFor(totalBytesRead))
                .append('/')
                .append(getStringDescriptionFor(source2.getLen()))
                .append(']');
        context.setStatus(message.toString());
    }

    public DecimalFormat getFormatter() {
        return FORMATTER.get();
    }

    /**
     * String utility to convert a number-of-bytes to human readable format.
     */
    private final ThreadLocal<DecimalFormat> FORMATTER
            = new ThreadLocal<DecimalFormat>() {
        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat("0.0");
        }
    };

    public String getStringDescriptionFor(long nBytes) {

        char units[] = {'B', 'K', 'M', 'G', 'T', 'P'};

        double current = nBytes;
        double prev = current;
        int index = 0;

        while ((current = current / 1024) >= 1) {
            prev = current;
            ++index;
        }

        assert index < units.length : "Too large a number.";

        return getFormatter().format(prev) + units[index];
    }


}
