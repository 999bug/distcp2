package org.apache.hadoop.tools.appendnf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.appendnf.dao.I2BBFileStatus;

import java.io.IOException;
import java.util.Objects;

/**
 * @Author: LiSY
 * @Date: 2021/10/22 17:20
 */
public class DistCpUtils {

    private static final Log logger = LogFactory.getLog(DistCpUtils.class);

    /**
     * Utility to compare checksums for the paths specified.
     * <p>
     * If checksums's can't be retrieved, it doesn't fail the test
     * Only time the comparison would fail is when checksums are
     * available and they don't match
     *
     * @param sourceFS       FileSystem for the source path.
     * @param source         The source path.
     * @return If either checksum couldn't be retrieved, the function returns
     * false. If checksums are retrieved, the function returns true if they match,
     * and false otherwise.
     */
    public static boolean checksumsAreEqual(FileSystem sourceFS, Path source, I2BBFileStatus status) {
        String sourceChe = null;
        String targetChe = null;
        FileChecksum sourceChecksum;
        try {
            sourceChecksum = sourceFS.getFileChecksum(source);
            sourceChe = sourceChecksum.toString().split(":")[1];
            targetChe = status.getCrc35();
            logger.warn("sourceChecksum " + sourceChe);
            logger.warn("targetChecksum " + targetChe);
        } catch (IOException e) {
            logger.error("Unable to retrieve checksum for " + source + " or " + status.getRelPath(), e);
        }
        return Objects.equals(sourceChe, targetChe);
    }
}
