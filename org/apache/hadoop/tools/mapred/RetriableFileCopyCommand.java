/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.mapred;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.mapred.CopyMapper.FileAction;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.tools.util.RetriableCommand;
import org.apache.hadoop.tools.util.ThrottledInputStream;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class extends RetriableCommand to implement the copy of files,
 * with retries on failure.
 */
public class RetriableFileCopyCommand extends RetriableCommand {

    private static Log logger = LogFactory.getLog(RetriableFileCopyCommand.class);
    private boolean skipCrc = false;
    private FileAction action;

    /**
     * Constructor, taking a description of the action.
     *
     * @param description Verbose description of the copy operation.
     */
    public RetriableFileCopyCommand(String description, FileAction action) {
        super(description);
        this.action = action;
    }

    /**
     * Create a RetriableFileCopyCommand.
     *
     * @param skipCrc     Whether to skip the crc check.
     * @param description A verbose description of the copy operation.
     * @param action      We should overwrite the target file or append new data to it.
     */
    public RetriableFileCopyCommand(boolean skipCrc, String description,
                                    FileAction action) {
        this(description, action);
        this.skipCrc = skipCrc;
    }

    /**
     * Implementation of RetriableCommand::doExecute().
     * This is the actual copy-implementation.
     *
     * @param arguments Argument-list to the command.
     * @return Number of bytes copied.
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Object doExecute(Object... arguments) throws Exception {
        assert arguments.length == 4 : "Unexpected argument list.";
        CopyListingFileStatus source = (CopyListingFileStatus) arguments[0];
        assert !source.isDirectory() : "Unexpected file-status. Expected file.";
        Path target = (Path) arguments[1];
        Mapper.Context context = (Mapper.Context) arguments[2];
        EnumSet<FileAttribute> fileAttributes
                = (EnumSet<FileAttribute>) arguments[3];
        return doCopy(source, target, context, fileAttributes);
    }

    private long doCopy(CopyListingFileStatus source, Path target,
                        Mapper.Context context, EnumSet<FileAttribute> fileAttributes)
            throws IOException {
        final boolean toAppend = action == FileAction.APPEND;
        Path targetPath = toAppend ? target : getTmpFile(target, context);
        logger.warn("targetPath " + targetPath);
        final Configuration configuration = context.getConfiguration();
        FileSystem targetFS = target.getFileSystem(configuration);

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Copying " + source.getPath() + " to " + target);
                logger.debug("Target file path: " + targetPath);
            }
            final Path sourcePath = source.getPath();
            final FileSystem sourceFS = sourcePath.getFileSystem(configuration);
            final FileChecksum sourceChecksum = fileAttributes.contains(FileAttribute.CHECKSUMTYPE) ?
                    sourceFS.getFileChecksum(sourcePath) : null;
            logger.warn("sourceChecksum " + sourceChecksum);
            long offset = (action == FileAction.APPEND) ?
                    targetFS.getFileStatus(target).getLen() : source.getChunkOffset();
            logger.warn("offset " + offset);
            long bytesRead = copyToFile(targetPath, targetFS, source,
                    offset, context, fileAttributes, sourceChecksum);

            logger.warn("sourceChecksum " + sourceChecksum);
            if (!source.isSplit()) {
                // ???????????????????????????????????????
                compareFileLengths(source, targetPath, configuration, bytesRead
                        + offset);
            }
            //At this point, src&dest lengths are same. if length==0, we skip checksum
            // ??????????????????src???dest???????????????????????? ????????????==0???????????????????????????
            if ((bytesRead != 0) && (!skipCrc)) {
                if (!source.isSplit()) {
                    compareCheckSums(sourceFS, source.getPath(), sourceChecksum,
                            targetFS, targetPath);
                }
            }
            // it's not append case, thus we first write to a temporary file, rename it to the target path.
            //???????????????????????????????????????????????????????????????????????????????????????????????????????????????
            if (!toAppend) {
                promoteTmpToTarget(targetPath, target, targetFS);
            }
            return bytesRead;

        } finally {
            // note that for append case, it is possible that we append partial data
            // and then fail. In that case, for the next retry, we either reuse the
            // partial appended data if it is good or we overwrite the whole file
           /*  ???????????????????????????????????????????????????????????????????????????????????????
           ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????*/
            if (!toAppend) {
                targetFS.delete(targetPath, false);
            }
        }
    }

    /**
     * @return the checksum spec of the source checksum if checksum type should be
     * preserved
     */
    private ChecksumOpt getChecksumOpt(EnumSet<FileAttribute> fileAttributes,
                                       FileChecksum sourceChecksum) {
        if (fileAttributes.contains(FileAttribute.CHECKSUMTYPE)
                && sourceChecksum != null) {
            return sourceChecksum.getChecksumOpt();
        }
        return null;
    }

    private long copyToFile(Path targetPath, FileSystem targetFS,
                            CopyListingFileStatus source, long sourceOffset, Mapper.Context context,
                            EnumSet<FileAttribute> fileAttributes, final FileChecksum sourceChecksum)
            throws IOException {
        // ???????????? permission
        FsPermission permission = FsPermission.getFileDefault().applyUMask(
                FsPermission.getUMask(targetFS.getConf()));
        // 8192
        int copyBufferSize = context.getConfiguration().getInt(
                DistCpOptionSwitch.COPY_BUFFER_SIZE.getConfigLabel(),
                DistCpConstants.COPY_BUFFER_SIZE_DEFAULT);
        logger.info("copyBufferSize " + copyBufferSize);
        final OutputStream outStream;
        if (action == FileAction.OVERWRITE) {
            final short repl = getReplicationFactor(fileAttributes, source,
                    targetFS, targetPath);
            final long blockSize = getBlockSize(fileAttributes, source,
                    targetFS, targetPath);// ??????128MB
            // ???????????????????????????
            FSDataOutputStream out = targetFS.create(targetPath, permission,
                    EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
                    copyBufferSize, repl, blockSize, context,
                    getChecksumOpt(fileAttributes, sourceChecksum));
            outStream = new BufferedOutputStream(out);
        } else {
            outStream = new BufferedOutputStream(targetFS.append(targetPath,
                    copyBufferSize));
        }

        return copyBytes(source, sourceOffset, outStream, copyBufferSize,
                context);
    }

    private void compareFileLengths(CopyListingFileStatus source, Path target,
                                    Configuration configuration, long targetLen)
            throws IOException {
        final Path sourcePath = source.getPath();
        FileSystem fs = sourcePath.getFileSystem(configuration);
        long srcLen = fs.getFileStatus(sourcePath).getLen();
        if (srcLen != targetLen)
            throw new IOException("Mismatch in length of source:" + sourcePath + " (" + srcLen +
                    ") and target:" + target + " (" + targetLen + ")");
    }

    private void compareCheckSums(FileSystem sourceFS, Path source,
                                  FileChecksum sourceChecksum, FileSystem targetFS, Path target)
            throws IOException {
        if (!DistCpUtils.checksumsAreEqual(sourceFS, source, sourceChecksum,
                targetFS, target)) {
            StringBuilder errorMessage = new StringBuilder("Check-sum mismatch between ")
                    .append(source).append(" and ").append(target).append(".");
            if (sourceFS.getFileStatus(source).getBlockSize() !=
                    targetFS.getFileStatus(target).getBlockSize()) {
                errorMessage.append(" Source and target differ in block-size.")
                        .append(" Use -pb to preserve block-sizes during copy.")
                        .append(" Alternatively, skip checksum-checks altogether, using -skipCrc.")
                        .append(" (NOTE: By skipping checksums, one runs the risk of " +
                                "masking data-corruption during file-transfer.)");
            }
            throw new IOException(errorMessage.toString());
        }
    }


    /**
     * ???????????????????????????????????????????????????--??????     //If target file exists and unable to delete target - fail
     * ?????????????????????????????????????????????????????????--??????   //If target doesn't exist and unable to create parent folder - fail
     * ??????????????????????????????????????????????????????????????????????????????--??????    //If target is successfully deleted and parent exists, if rename fails - fail
     */
    private void promoteTmpToTarget(Path tmpTarget, Path target, FileSystem fs)
            throws IOException {
        if ((fs.exists(target) && !fs.delete(target, false))
                || (!fs.exists(target.getParent()) && !fs.mkdirs(target.getParent()))
                || !fs.rename(tmpTarget, target)) {
            throw new IOException("Failed to promote tmp-file:" + tmpTarget
                    + " to: " + target);
        }
    }

    private Path getTmpFile(Path target, Mapper.Context context) {
        Path targetWorkPath = new Path(context.getConfiguration().
                get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
        logger.warn("targetWorkPath: " + targetWorkPath);
        Path root = target.equals(targetWorkPath) ? targetWorkPath.getParent() : targetWorkPath;
        logger.info("Creating temp file: " +
                new Path(root, ".distcp.tmp." + context.getTaskAttemptID().toString()));
        return new Path(root, ".distcp.tmp." + context.getTaskAttemptID().toString());
    }
static  int a = 1;
    @VisibleForTesting
    long copyBytes(CopyListingFileStatus source2, long sourceOffset,
                   OutputStream outStream, int bufferSize, Mapper.Context context)
            throws IOException {

        Path source = source2.getPath();
        byte buf[] = new byte[bufferSize];
        // ????????????????????? ?????????
        ThrottledInputStream inStream = null;
        long totalBytesRead = 0;

        long chunkLength = source2.getChunkLength();
        boolean finished = false;
        try {
            inStream = getInputStream(source, context.getConfiguration());
            int bytesRead = readBytes(inStream, buf, sourceOffset);
            while (bytesRead >= 0) {
                if (chunkLength > 0 && (totalBytesRead + bytesRead) >= chunkLength) {
                    bytesRead = (int) (chunkLength - totalBytesRead);
                    finished = true;
                }
                totalBytesRead += bytesRead;
                // append
                if (action == FileAction.APPEND) {
                    sourceOffset += bytesRead;
                }
                 logger.warn("????????????????????????" + a++);
                outStream.write(buf, 0, bytesRead);
                // ?????? map ?????????
                updateContextStatus(totalBytesRead, context, source2);
                if (finished) {
                    break;
                }
                bytesRead = readBytes(inStream, buf, sourceOffset);
            }
            outStream.close();
            outStream = null;
        } finally {
            IOUtils.cleanup(logger, outStream, inStream);
        }
        return totalBytesRead;
    }

    private void updateContextStatus(long totalBytesRead, Mapper.Context context,
                                     CopyListingFileStatus source2) {
        StringBuilder message = new StringBuilder(DistCpUtils.getFormatter()
                .format(totalBytesRead * 100.0f / source2.getLen()));
        message.append("% ")
                .append(description).append(" [")
                .append(DistCpUtils.getStringDescriptionFor(totalBytesRead))
                .append('/')
                .append(DistCpUtils.getStringDescriptionFor(source2.getLen()))
                .append(']');
        context.setStatus(message.toString());
    }

    private static int readBytes(ThrottledInputStream inStream, byte buf[], long position) throws IOException {
        try {
            if (position == 0) {
                return inStream.read(buf);
            } else {
                return inStream.read(position, buf, 0, buf.length);
            }
        } catch (IOException e) {
            throw new CopyReadException(e);
        }
    }

    private static ThrottledInputStream getInputStream(Path path,
                                                       Configuration conf) throws IOException {
        try {
            FileSystem fs = path.getFileSystem(conf);
            long bandwidthMB = conf.getInt(DistCpConstants.CONF_LABEL_BANDWIDTH_MB,
                    DistCpConstants.DEFAULT_BANDWIDTH_MB);
            logger.warn("bandwidthMB " + bandwidthMB);
            FSDataInputStream in = fs.open(path);
            return new ThrottledInputStream(in, bandwidthMB * 1024 * 1024);
        } catch (IOException e) {
            throw new CopyReadException(e);
        }
    }

    private static short getReplicationFactor(
            EnumSet<FileAttribute> fileAttributes, CopyListingFileStatus source,
            FileSystem targetFS, Path tmpTargetPath) {
        return fileAttributes.contains(FileAttribute.REPLICATION)
                ? source.getReplication()
                : targetFS.getDefaultReplication(tmpTargetPath);
    }

    /**
     * @return the block size of the source file if we need to preserve either
     * the block size or the checksum type. Otherwise the default block
     * size of the target FS.
     */
    private static long getBlockSize(
            EnumSet<FileAttribute> fileAttributes, CopyListingFileStatus source,
            FileSystem targetFS, Path tmpTargetPath) {
        boolean preserve = fileAttributes.contains(FileAttribute.BLOCKSIZE)
                || fileAttributes.contains(FileAttribute.CHECKSUMTYPE);
        return preserve ? source.getBlockSize() : targetFS
                .getDefaultBlockSize(tmpTargetPath);
    }

    /**
     * Special subclass of IOException. This is used to distinguish read-operation
     * failures from other kinds of IOExceptions.
     * The failure to read from source is dealt with specially, in the CopyMapper.
     * Such failures may be skipped if the DistCpOptions indicate so.
     * Write failures are intolerable, and amount to CopyMapper failure.
     */
    @SuppressWarnings("serial")
    public static class CopyReadException extends IOException {
        public CopyReadException(Throwable rootCause) {
            super(rootCause);
        }
    }
}
