# mysql binglog分析脚本
使用：
1.下载脚本后使用go build 编译

2.使用示例:
  ./mysqlbin --help
Usage of ./mysqlbin:
  -f string
        binglog file path eg binlog -f D:/binlog/binlog.000056
 
3.输出结果示例:
  4021b155ba7d11e9802e5254006fddd6:448110 [{txc t1 Update} {test t2 Update}] start_pos: 234 end_pos: 622 event_length 388  start_time: 2019-12-25 11:07:59 commit_time: 2019-12-25 11:08:17 trx_exe_long: 18 exe_time: 0
	
 4021b155ba7d11e9802e5254006fddd6:448110  ------ gtid 
 
 {txc t1 Update}   ----dbname txc table t1 DML类型 update
 
 event_lengt =end_pos-start_pos (单位byte) 
 
 start_time 就是query event 的时间
 
 commit_time gtid event 的时间
 
 trx_exe_long = commit_time-start_time(单位second)
 
 exe_time query event 里面的exec_time
 
 使用限制：必须打开gtid
