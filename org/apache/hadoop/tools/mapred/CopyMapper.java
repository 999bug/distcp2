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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.mapred.RetriableFileCopyCommand.CopyReadException;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper class that executes the DistCp copy operation.
 * Implements the o.a.h.mapreduce.Mapper interface.
 */
public class CopyMapper extends Mapper<Text, CopyListingFileStatus, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(CopyMapper.class);

    /**
     * Hadoop counters for the DistCp CopyMapper.
     * (These have been kept identical to the old DistCp,
     * for backward compatibility.)
     */
    public static enum Counter {
        COPY,         // Number of files received by the mapper for copy.
        DIR_COPY,     // Number of directories received by the mapper for copy.
        SKIP,         // Number of files skipped.
        FAIL,         // Number of files that failed to be copied.
        BYTESCOPIED,  // Number of bytes actually copied by the copy-mapper, total.
        BYTESEXPECTED,// Number of bytes expected to be copied.
        BYTESFAILED,  // Number of bytes that failed to be copied.
        BYTESSKIPPED, // Number of bytes that were skipped from copy.
    }

    /**
     * Indicate the action for each file
     */
    static enum FileAction {
        SKIP,         // Skip copying the file since it's already in the target FS
        APPEND,       // Only need to append new data to the file in the target FS
        OVERWRITE,    // Overwrite the whole file
    }

    private static Log LOG = LogFactory.getLog(CopyMapper.class);

    private Configuration conf;

    private boolean syncFolders = false;
    private boolean ignoreFailures = false;
    private boolean skipCrc = false;
    private boolean overWrite = false;
    private boolean append = false;
    private boolean verboseLog = false;
    private EnumSet<FileAttribute> preserve = EnumSet.noneOf(FileAttribute.class);

    private FileSystem targetFS = null;
    private Path targetWorkPath = null;

    /**
     * Implementation of the Mapper::setup() method. This extracts the DistCp-
     * options specified in the Job's configuration, to set up the Job.
     * @param context Mapper's context.
     * @throws IOException On IO failure.
     * @throws InterruptedException If the job is interrupted.
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        // update ????????????
        syncFolders = conf.getBoolean(DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(), false);
        logger.warn("syncFolders {}", syncFolders);
        ignoreFailures = conf.getBoolean(DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(), false);
        skipCrc = conf.getBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(), false);
        overWrite = conf.getBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(), false);
        logger.warn("overWrite start {}", overWrite);
        append = conf.getBoolean(DistCpOptionSwitch.APPEND.getConfigLabel(), false);
        logger.warn("append {}", append);
        verboseLog = conf.getBoolean(
                DistCpOptionSwitch.VERBOSE_LOG.getConfigLabel(), false);
        logger.warn("verboseLog {}", verboseLog);
        preserve = DistCpUtils.unpackAttributes(conf.get(DistCpOptionSwitch.
                PRESERVE_STATUS.getConfigLabel()));
        logger.warn("preserve {}", preserve);

        targetWorkPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
        logger.warn("targetWorkPath {}", targetWorkPath);
        Path targetFinalPath = new Path(conf.get(
                DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
        logger.warn("targetFinalPath {}", targetFinalPath);
        targetFS = targetFinalPath.getFileSystem(conf);
        logger.warn("targetFS {}", targetFS);

        try {
            overWrite = overWrite || targetFS.getFileStatus(targetFinalPath).isFile();
        } catch (FileNotFoundException ignored) {
        }
        logger.warn("overWrite end {}", overWrite);

        if (conf.get(DistCpConstants.CONF_LABEL_SSL_CONF) != null) {
            initializeSSLConf(context);
        }

    }

    /**
     * Initialize SSL Config if same is set in conf
     *
     * @throws IOException - If any
     */
    private void initializeSSLConf(Context context) throws IOException {
        LOG.info("Initializing SSL configuration");

        String workDir = conf.get(JobContext.JOB_LOCAL_DIR) + "/work";
        Path[] cacheFiles = context.getLocalCacheFiles();

        Configuration sslConfig = new Configuration(false);
        String sslConfFileName = conf.get(DistCpConstants.CONF_LABEL_SSL_CONF);
        Path sslClient = findCacheFile(cacheFiles, sslConfFileName);
        if (sslClient == null) {
            LOG.warn("SSL Client config file not found. Was looking for " + sslConfFileName +
                    " in " + Arrays.toString(cacheFiles));
            return;
        }
        sslConfig.addResource(sslClient);

        String trustStoreFile = conf.get("ssl.client.truststore.location");
        Path trustStorePath = findCacheFile(cacheFiles, trustStoreFile);
        sslConfig.set("ssl.client.truststore.location", trustStorePath.toString());

        String keyStoreFile = conf.get("ssl.client.keystore.location");
        Path keyStorePath = findCacheFile(cacheFiles, keyStoreFile);
        sslConfig.set("ssl.client.keystore.location", keyStorePath.toString());

        try {
            OutputStream out = new FileOutputStream(workDir + "/" + sslConfFileName);
            try {
                sslConfig.writeXml(out);
            } finally {
                out.close();
            }
            conf.set(DistCpConstants.CONF_LABEL_SSL_KEYSTORE, sslConfFileName);
        } catch (IOException e) {
            LOG.warn("Unable to write out the ssl configuration. " +
                    "Will fall back to default ssl-client.xml in class path, if there is one", e);
        }
    }

    /**
     * Find entry from distributed cache
     *
     * @param cacheFiles - All localized cache files
     * @param fileName - fileName to search
     * @return Path of the filename if found, else null
     */
    private Path findCacheFile(Path[] cacheFiles, String fileName) {
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (Path file : cacheFiles) {
                if (file.getName().equals(fileName)) {
                    return file;
                }
            }
        }
        return null;
    }

    private static int a = 0;

    /**
     * Implementation of the Mapper::map(). Does the copy.
     * @param relPath The target path.
     * @param sourceFileStatus The source path.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Text relPath, CopyListingFileStatus sourceFileStatus,
                    Context context) throws IOException, InterruptedException {
        Path sourcePath = sourceFileStatus.getPath();
        logger.warn("===========map {} start!==============", a);
        logger.info("sourcePath {}", sourcePath);
        if (LOG.isDebugEnabled())
            LOG.debug("DistCpMapper::map(): Received " + sourcePath + ", " + relPath);

        Path target = new Path(targetWorkPath.makeQualified(targetFS.getUri(),
                targetFS.getWorkingDirectory()) + relPath.toString());
        logger.info("target {}", target);

        EnumSet<DistCpOptions.FileAttribute> fileAttributes
                = getFileAttributeSettings(context);
        final boolean preserveRawXattrs = context.getConfiguration().getBoolean(
                DistCpConstants.CONF_LABEL_PRESERVE_RAWXATTRS, false);

        final String description = "Copying " + sourcePath + " to " + target;
        context.setStatus(description);

        LOG.info(description);

        try {
            CopyListingFileStatus sourceCurrStatus;
            FileSystem sourceFS;
            try {
                sourceFS = sourcePath.getFileSystem(conf);
                final boolean preserveXAttrs =
                        fileAttributes.contains(FileAttribute.XATTR);
                // ?????????????????????
                sourceCurrStatus = DistCpUtils.toCopyListingFileStatusHelper(sourceFS,
                        sourceFS.getFileStatus(sourcePath),
                        fileAttributes.contains(FileAttribute.ACL),
                        preserveXAttrs, preserveRawXattrs,
                        sourceFileStatus.getChunkOffset(),
                        sourceFileStatus.getChunkLength());
            } catch (FileNotFoundException e) {
                throw new IOException(new RetriableFileCopyCommand.CopyReadException(e));
            }

            FileStatus targetStatus = null;

            try {
                targetStatus = targetFS.getFileStatus(target);
            } catch (FileNotFoundException ignore) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Path could not be found: " + target, ignore);
            }

            if (targetStatus != null &&
                    (targetStatus.isDirectory() != sourceCurrStatus.isDirectory())) {
                throw new IOException("Can't replace " + target + ". Target is " +
                        getFileType(targetStatus) + ", Source is " + getFileType(sourceCurrStatus));
            }
            // ????????????????????????????????????????????????
            if (sourceCurrStatus.isDirectory()) {
                createTargetDirsWithRetry(description, target, context);
                return;
            }
            // ?????? ??????Action
            FileAction action = checkUpdate(sourceFS, sourceCurrStatus, target,
                    targetStatus);

            Path tmpTarget = target;
            if (action == FileAction.SKIP) {
                LOG.info("Skipping copy of " + sourceCurrStatus.getPath()
                        + " to " + target);
                updateSkipCounters(context, sourceCurrStatus);
                context.write(null, new Text("SKIP: " + sourceCurrStatus.getPath()));

                if (verboseLog) {
                    context.write(null,
                            new Text("FILE_SKIPPED: source=" + sourceFileStatus.getPath()
                                    + ", size=" + sourceFileStatus.getLen() + " --> "
                                    + "target=" + target + ", size=" + (targetStatus == null ?
                                    0 : targetStatus.getLen())));
                }
            } else {// overwrite ??? append ??????????????????
                if (sourceCurrStatus.isSplit()) {
                    tmpTarget = DistCpUtils.getSplitChunkPath(target, sourceCurrStatus);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("copying " + sourceCurrStatus + " " + tmpTarget);
                }// ???????????????????????????
                copyFileWithRetry(description, sourceCurrStatus, tmpTarget, targetStatus, context, action, fileAttributes);
            }
            DistCpUtils.preserve(target.getFileSystem(conf), tmpTarget, sourceCurrStatus, fileAttributes, preserveRawXattrs);
            logger.warn("===========map {} end!==============", a++);
        } catch (IOException exception) {
            handleFailures(exception, sourceFileStatus, target, context);
        }

    }

    private String getFileType(CopyListingFileStatus fileStatus) {
        if (null == fileStatus) {
            return "N/A";
        }
        return fileStatus.isDirectory() ? "dir" : "file";
    }

    private String getFileType(FileStatus fileStatus) {
        if (null == fileStatus) {
            return "N/A";
        }
        return fileStatus.isDirectory() ? "dir" : "file";
    }

    private static EnumSet<DistCpOptions.FileAttribute>
    getFileAttributeSettings(Mapper.Context context) {
        String attributeString = context.getConfiguration().get(
                DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel());
        return DistCpUtils.unpackAttributes(attributeString);
    }

    private void copyFileWithRetry(String description,
                                   CopyListingFileStatus sourceFileStatus, Path target,
                                   FileStatus targrtFileStatus, Context context, FileAction action,
                                   EnumSet<DistCpOptions.FileAttribute> fileAttributes)
            throws IOException, InterruptedException {

        long bytesCopied;
        try {
            bytesCopied = (Long) new RetriableFileCopyCommand(skipCrc, description, action)
                    .execute(sourceFileStatus, target, context, fileAttributes);
        } catch (Exception e) {
            context.setStatus("Copy Failure: " + sourceFileStatus.getPath());
            throw new IOException("File copy failed: " + sourceFileStatus.getPath() +
                    " --> " + target, e);
        }
        incrementCounter(context, Counter.BYTESEXPECTED, sourceFileStatus.getLen());
        incrementCounter(context, Counter.BYTESCOPIED, bytesCopied);
        incrementCounter(context, Counter.COPY, 1);

        if (verboseLog) {
            context.write(null,
                    new Text("FILE_COPIED: source=" + sourceFileStatus.getPath() + ","
                            + " size=" + sourceFileStatus.getLen() + " --> " + "target="
                            + target + ", size=" + (targrtFileStatus == null ?
                            0 : targrtFileStatus.getLen())));
        }
    }

    private void createTargetDirsWithRetry(String description,
                                           Path target, Context context) throws IOException {
        try {
            new RetriableDirectoryCreateCommand(description).execute(target, context);
        } catch (Exception e) {
            throw new IOException("mkdir failed for " + target, e);
        }
        incrementCounter(context, Counter.DIR_COPY, 1);
    }

    private static void updateSkipCounters(Context context,
                                           CopyListingFileStatus sourceFile) {
        incrementCounter(context, Counter.SKIP, 1);
        incrementCounter(context, Counter.BYTESSKIPPED, sourceFile.getLen());

    }

    private void handleFailures(IOException exception,
                                CopyListingFileStatus sourceFileStatus, Path target, Context context)
            throws IOException, InterruptedException {
        LOG.error("Failure in copying " + sourceFileStatus.getPath() +
                (sourceFileStatus.isSplit() ? ","
                        + " offset=" + sourceFileStatus.getChunkOffset()
                        + " chunkLength=" + sourceFileStatus.getChunkLength()
                        : "") +
                " to " + target, exception);

        if (ignoreFailures &&
                ExceptionUtils.indexOfType(exception, CopyReadException.class) != -1) {
            incrementCounter(context, Counter.FAIL, 1);
            incrementCounter(context, Counter.BYTESFAILED, sourceFileStatus.getLen());
            context.write(null, new Text("FAIL: " + sourceFileStatus.getPath() + " - " +
                    StringUtils.stringifyException(exception)));
        } else
            throw exception;
    }

    private static void incrementCounter(Context context, Counter counter,
                                         long value) {
        context.getCounter(counter).increment(value);
    }
    // CopyListingFileStatus ????????????????????? length:3205,isDir:false,blockSize:134563,modificationTime:,??????
    private FileAction checkUpdate(FileSystem sourceFS,
                                   CopyListingFileStatus source, Path target, FileStatus targetFileStatus)
            throws IOException {
        if (targetFileStatus != null && !overWrite) {
            if (canSkip(sourceFS, source, targetFileStatus)) {
                return FileAction.SKIP;
            } else if (append) {
                logger.warn("append ???????????? " + append);
                long targetLen = targetFileStatus.getLen();
                if (targetLen < source.getLen()) {
                    FileChecksum sourceChecksum = sourceFS.getFileChecksum(source.getPath(), targetLen);
                    if (sourceChecksum != null && sourceChecksum.equals(targetFS.getFileChecksum(target))) {
                        // We require that the checksum is not null. Thus currently only
                        // DistributedFileSystem is supported
                        return FileAction.APPEND;
                    }
                }
            }
        }
        return FileAction.OVERWRITE;
    }
    // ?????????????????????????????????
    private boolean canSkip(FileSystem sourceFS, CopyListingFileStatus source,
                            FileStatus target) throws IOException {
        if (!syncFolders) {
            return true;
        }
        boolean sameLength = target.getLen() == source.getLen();
        boolean sameBlockSize = source.getBlockSize() == target.getBlockSize() || !preserve.contains(FileAttribute.BLOCKSIZE);
        logger.warn("source BlockSize " +source.getBlockSize());
        logger.warn("target BlockSize() " +target.getBlockSize());

        logger.warn("skip " +  DistCpUtils.checksumsAreEqual(sourceFS, source.getPath(), null, targetFS, target.getPath()));
        if (sameLength && sameBlockSize) {
            return skipCrc ||
                    DistCpUtils.checksumsAreEqual(sourceFS, source.getPath(), null, targetFS, target.getPath());

        } else {
            return false;
        }
    }
}
