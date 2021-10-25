package org.apache.hadoop.tools.appendnf.consts;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.tools.appendnf.dao.I2BBFileStatus;

/**
 * @Author: LiSY
 * @Date: 2021/10/25 15:34
 */
@Getter
@Setter
public class TransDTO {

    private I2BBFileStatus status;
    private  I2bbFileAction action;

    public TransDTO(I2BBFileStatus status, I2bbFileAction action) {
        this.status = status;
        this.action = action;
    }

}
