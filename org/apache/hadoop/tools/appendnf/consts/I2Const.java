package org.apache.hadoop.tools.appendnf.consts;

public interface I2Const {

	int GET_FILE_STATUS = 10086;
	int TRANSFER_FILE = 10099;

	//split
	String PH = "%s";
	String SP = ",";
	String MSP = ";";
	String LSP = " ";
	String FSP = "#";
	String SSP = "-";
	
	//rule
	String bakRuleCfgPath = "/etc/bakrule.dat";
	String recRuleCfgPath = "/etc/recrule.dat";
	
    //define err NO.
    int OK = 0;
    int E_OFF = 6024;
    int E_OK = E_OFF + 0x0;//operation succeed
    
    //no bak data 20200313
    // 7001 备份根路径是正确的，但备份根路径无可恢复的数据，可能是没备份完或者其他原因
    // 7002 选择的备份路径是错误的
    // 7003 选择的时间点无备份数据
    String E_NOBAKDATA = "7001";
    String E_BAKPATHERR = "7002";
    String E_TIMENOBAK = "7003";
    // i2bb.conf cluster full path error 
    int E_NOCONFFILE = 7004;
    int E_HJOBNOTSTARTED = 7005;//hjob not started
    int E_MANUALHDFSPATH = 7006;//manual input hdfs path error
    
    int E_MAPPERFAILED = 7007;//mapper failed return code not 0
    int E_BAKFILELISTEMPTY = 7008;//bak filelist is empty will not execute mapper
    int E_NODISKSPACE = 7009;//No enough space left for the file
    int E_ACCESS_CONTROL = 7010;//Access denied for user root. Superuser privilege is required
    
	//os related
	String CHARSET = "UTF-8";
	String LINESEPER = System.lineSeparator();
	
    //	io
    int E_CREATEFILE = E_OFF + 0x06;
    int E_FILENOTFOUND = E_OFF + 0x07;
    int E_OPENFILE = E_OFF + 0x08;
    int E_RDFILE = E_OFF + 0x09;
    int E_WRFILE = E_OFF + 0x0A;
    int E_CLOSEFILE = E_OFF + 0x0B;
    int E_DELFILE = E_OFF + 0x0C;
    int E_FORMATDATE = E_OFF + 0x0D;
    int E_DECODE = E_OFF + 0x0E;
    int E_NUMFORMAT = E_OFF + 0x0F;
    int E_INVALIDOP = E_OFF + 0x10;
    
    //rule status
    int RS_INIT = 0;
    int RS_RUNNING = 1;
    int RS_STOP = 2;
    int RS_OVER = 3;
    
    
	//define state for bak rule
	int RL_STAT_DONE = 0x00;//finish
	int RL_STAT_STOP = 0x01;//stop
	int RL_STAT_START = 0x02;//running
	int RL_STAT_PENDING = 0x03;
	
	int RL_STAT_QTSK = 0x04;
	int RL_STAT_SUM = 0x05;
	int RL_STAT_RETRY = 0x06;
	int RL_STAT_TRNS = 0x07;
	int RL_STAT_MGLC = 0x08;
	int RL_STAT_GUARD = 0x09;
	//int RL_STAT_DONE = 0x0A;
	int RL_STAT_CRVM = 0x0B;
	int RL_STAT_ERUL = 0x0C;
	int RL_STAT_MOVE = 0x0D;
	int RL_STAT_STAL = 0x0E;
	int RL_STAT_RVT = 0x0F;
	int RL_STAT_PEND = 0x10;
	int RL_STAT_RUN = 0x11;
	int RL_STAT_FAILOVERING = 0x12;
	int RL_STAT_FAILOVERED = 0x13;
    
    //define scheduled task type
    int SCHD_FIX_ONE_TIME = 0x0;
    int SCHD_DELAY_CYCLE = 0x1;
    int SCHD_DELAY_ONE_TIME = 0x2;
    int SCHD_FIX_CYCLE = 0x3;
    int SCHD_FR_FIX_CYCLE = 0x4;
    int SCHD_FR_DELAY_CYCLE = 0x5;
    int SCHD_CYCLE = 0x6;
    
    int E_INVALIDSCHED = E_OFF + 0x3C;
    
    //copyied from I2config
	int OPT_RETRY_TIMES =5; //"Retry_Times";
	int OPT_RETRY_INTERVAL =5; //"Retry_Interval";
	
	
	//rule policy
	char PTP_F = 'f';
	char PTP_O = 'o';
	char PTP_I = 'i';
	char PTP_D = 'd';
	char PTP_R = 'r';
	
	char PLCY_IMMEDIATELY = 'i';
	char PLCY_ONE = 'o';
	char PLCY_DALIY = 'd';
	char PLCY_WEEKLY = 'w';
	char PLCY_MONTHLY = 'm';
	char PLCY_SEASONLY = 's';
	
	//define policy type start
	int PLCY_SCHE = 0xF00;
	int PLCY_CYCL = 0xE00;
	int PLCY_SCHE_MD =0x10;
	int PLCY_SCHE_WD =0x20;
	int PLCY_SCHE_D =0x40;
	//	conflict dealing
	int CONF_WAIT = 0x01;
	int CONF_GVUP = 0x02;
	
	int DefaultBakVerLimit = 255;
	int DefaultBakPerLimit = 64;
	int DEFAULT_TCODE = 0x1F;
	
	String PolicyDateFormat = "yyyy-MM-dd HH:mm:ss";
	//define policy type end
	
	char LOG_LEVEL_DEFAULT = '3';
	char LOG_LEVEL_INFO = '2';
	char LOG_LEVEL_WARN = '1';
	char LOG_LEVEL_ERR = '0';
	
    //schedule target type
    int SCHED_TARGET_DEFAULT = 0;
    int SCHED_TARGET_PAUSE = 1;
    int SCHED_TARGET_RULE = 2;
    
    //for log
    int E_CONNBAKSVR = E_OFF + 0x36;
	int RPC_REQ_TO = 10000;
	int RPC_RPLY_TO = 300000;
	String CC_LOG_URL = "/i2/i2/log_server.php";
	String CC_LOG_I = "log_server";
	int CC_PORT = 0xE2E0;
	String DEFAULTID = "00000000-0000-0000-0000-000000000000";
	//and statics
    int TSKSUCCESS = 0x0;
    int TSKCANCEL = 0x1;
    int TSKSKIP = 0x2;
    int TSKSTOP = 0x65;
    int TSKFAILED = 0x66;
    int TSKNOTFOUND = 0x67;
    
}
