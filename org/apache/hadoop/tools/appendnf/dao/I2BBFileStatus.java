package org.apache.hadoop.tools.appendnf.dao;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class I2BBFileStatus  {

	private static final long serialVersionUID = 1L;

	private String sourcePath;
	private String relPath;
	private boolean trunk;
	private long trunkSize;
	private long lastUpdateTime;
	private String host;
	private int port;
	private long seqId;
	private long offset;
	private boolean draft;
	//rec find bakno flag
	private boolean found = false;
	//add by szq
	private String md5HexStr;
	private String rluuid;

	private String crc35;
	private String fileName;

	public I2BBFileStatus() {

	}

	public I2BBFileStatus(I2BBFileStatus fStatus) {
//		this.sourcePath = fStatus.sourcePath;
//		this.relPath = fStatus.relPath;
		this.sourcePath = fStatus.sourcePath.replace("/.snapshot/snapshotName9527", "");
		this.relPath = fStatus.relPath.replace("/.snapshot/snapshotName9527", "");
		this.trunk = fStatus.trunk;
		this.trunkSize = fStatus.trunkSize;
		this.lastUpdateTime = fStatus.lastUpdateTime;
		this.host = fStatus.host;
		this.port = fStatus.port;
		this.seqId = fStatus.seqId;
		this.offset = fStatus.offset;
		this.draft = fStatus.draft;
		this.md5HexStr = fStatus.md5HexStr;
		this.rluuid = fStatus.rluuid;
	}




	private static Type typeOfI2BBFileStatus = new TypeToken<I2BBFileStatus>(){}.getType();
	public static I2BBFileStatus toI2BBFileStatus(String fStatus) {
		Gson json = new Gson();
		return json.fromJson(fStatus, typeOfI2BBFileStatus);
	}

	private static Type typeOfI2BBFileStatusMap = new TypeToken<Map<String, I2BBFileStatus>>(){}.getType();
	public static Map<String, I2BBFileStatus> toI2BBFileStatusMap(String strJSon) {
		Gson json = new Gson();
		return json.fromJson(strJSon, typeOfI2BBFileStatusMap);
	}
}
