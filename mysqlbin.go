package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)

type Event_Header struct {
	Timestamp     uint32
	Type_code     uint8
	Server_id     uint32
	Event_length  uint32
	Next_position uint32
	Flags         uint16
}

var data Event_Header
var bf bytes.Buffer
var by []byte
var st_pos int
var ed_pos uint32
var cur_gtid string
var cur_schema string
var cur_table string
var cur_dml_type string
var Last_que_pos uint32
var Exist_xid bool

type Sch_table struct {
	schema     string
	table_name string
	dml_type   string
}
type Pos_total struct {
	start_pos int
	end_pos   uint32
}

//var Txc_ = make(map[string]map[Sch_table]Pos_total)
var gtid_st = make(map[string][]Sch_table)
var Pos = make(map[string]Pos_total)
var dt = make(map[string]uint32)
var commit_tim = make(map[string]uint32)
var exetime = make(map[string]uint32)

func main() {
	input := flag.String("f", "", "binglog file path "+
		"eg binlog -f D:/binlog/binlog.000056")
	flag.Parse()
	if *input == "" {
		fmt.Println("please input binglog file! eg:./binlog -f D:/binlog/binlog.000056")
		return
	}
	st := time.Now()
	file, err := os.OpenFile(*input, os.O_RDONLY, 0600)
	if err != nil {
		fmt.Println("file open error", err)
		return
	}
	defer file.Close()
	_, _ = file.Seek(4, 0)
	_, err = bf.ReadFrom(file)
	if err == io.EOF {
		return
	} else {
		for {
			event_hader()
			if bf.Len() == 0 {
				break
			}
		}

	}
	Ana_Map(Pos)
	end := time.Now()
	fmt.Println(end.Sub(st))

}
func event_hader() {
	evt := bf.Next(19)
	sf := bytes.NewReader(evt)
	if err := binary.Read(sf, binary.LittleEndian, &data); err != nil {
		fmt.Println("binary.Read failed:", err)
		return
	}
	//timestamp :=fmt.Sprintf("%s", time.Unix(int64(data.Timestamp), 0).Format("2006-01-02 15:04:05"))
	/*
		//timestamp :=fmt.Sprintf("%s", time.Unix(int64(data.Timestamp), 0).Format("2006-01-02 15:04:05"))
		fmt.Println("Timestamp:", time.Unix(int64(data.Timestamp), 0).Format("2006-01-02 15:04:05"))
		fmt.Println("Type_code:", data.Type_code)
		fmt.Println("Server_id:", data.Server_id)
		fmt.Println("Event_length:", data.Event_length)
		fmt.Println("Next_position:", data.Next_position)
		fmt.Println("Flags:", data.Flags) */
	event_data(data.Type_code, int(data.Event_length), data.Next_position, data.Timestamp)
	//	fmt.Println("#########################分割线##################################")
}
func event_data(type_code uint8, data_length int, next_pos uint32, tim uint32) {
	switch type_code {
	case 2:
		type Que_Event struct {
			Thread_id    uint32
			Exec_time    uint32
			Database_len uint8
			Err_code     uint16
			Status       uint16
		}
		var que Que_Event
		Edata := bf.Next(data_length - 19)
		be := bytes.NewReader(Edata)
		err := binary.Read(be, binary.LittleEndian, &que)
		if err != nil {
			fmt.Println("que binary.Read failed:", err)
			return
		}
		/*
			fmt.Println("Thread_id:", que.Thread_id)
			fmt.Println("Exec_time:", que.Exec_time)
			fmt.Println("Database_len:", que.Database_len)
			fmt.Println("Err_code :", que.Err_code) */
		Last_que_pos = next_pos
		dt[cur_gtid] = tim
		//fmt.Println("Exec_time:", que.Exec_time)
		exetime[cur_gtid] = que.Exec_time
	case 19:
		var Table_Map_Event_Fix struct {
			Table_id   [6]byte
			Reserved   [2]byte
			Dbname_len uint8
		}
		Edata := bf.Next(data_length - 19)
		be := bytes.NewReader(Edata)
		err := binary.Read(be, binary.LittleEndian, &Table_Map_Event_Fix)
		if err != nil {
			fmt.Println("map binary.Read failed:", err)
			return
		}
		//	fmt.Println("Table_id:", Table_Map_Event_Fix.Table_id[0])
		//	fmt.Println("Dbname_len:", Table_Map_Event_Fix.Dbname_len)
		cur_schema = string(Edata[9 : 9+Table_Map_Event_Fix.Dbname_len])
		//	fmt.Println("schema:", string(Edata[9:9+Table_Map_Event_Fix.Dbname_len]))
		table_len_index := 9 + Table_Map_Event_Fix.Dbname_len + 1
		table_len := Edata[table_len_index]
		table_len_end := table_len_index + 1 + table_len
		cur_table = string(Edata[table_len_index+1 : table_len_end])
		//	fmt.Println("table_name:", string(Edata[table_len_index+1:table_len_end]))

	case 30:
		var Write_Rows_Event struct {
			Table_id [6]byte
			Reserved uint16
		}
		Edata := bf.Next(data_length - 19)
		be := bytes.NewReader(Edata)
		err := binary.Read(be, binary.LittleEndian, &Write_Rows_Event)
		if err != nil {
			fmt.Println("insert binary.Read failed:", err)
			return
		}
		//fmt.Println("Table_id:", Write_Rows_Event.Table_id[0])
		//fmt.Println(string(Edata[9:]))
		cur_dml_type = "Insert"
		if v, ok := gtid_st[cur_gtid]; !ok {
			v = append(v, Sch_table{cur_schema, cur_table, cur_dml_type})
			gtid_st[cur_gtid] = v
		} else {
			v = append(v, Sch_table{cur_schema, cur_table, cur_dml_type})
			gtid_st[cur_gtid] = v
		}
	case 31:
		var Update_Rows_Event struct {
			Table_id [6]byte
			Reserved uint16
		}
		Edata := bf.Next(data_length - 19)
		be := bytes.NewReader(Edata[0:9])
		err := binary.Read(be, binary.LittleEndian, &Update_Rows_Event)
		if err != nil {
			fmt.Println("update binary.Read failed:", err)
			return
		}
		//fmt.Println("Table_id:", Update_Rows_Event.Table_id[0])
		cur_dml_type = "Update"
		if v, ok := gtid_st[cur_gtid]; !ok {
			v = append(v, Sch_table{cur_schema, cur_table, cur_dml_type})
			gtid_st[cur_gtid] = v
		} else {
			v = append(v, Sch_table{cur_schema, cur_table, cur_dml_type})
			gtid_st[cur_gtid] = v
		}

	case 32:
		var Delete_Rows_Event struct {
			Table_id [6]byte
			Reserved uint16
		}
		Edata := bf.Next(data_length - 19)
		be := bytes.NewReader(Edata[0:9])
		err := binary.Read(be, binary.LittleEndian, &Delete_Rows_Event)
		if err != nil {
			fmt.Println("delete binary.Read failed:", err)
			return
		}
		//fmt.Println("Table_id:", Delete_Rows_Event.Table_id[0])
		cur_dml_type = "Delete"
		if v, ok := gtid_st[cur_gtid]; !ok {
			v = append(v, Sch_table{cur_schema, cur_table, cur_dml_type})
			gtid_st[cur_gtid] = v
		} else {
			v = append(v, Sch_table{cur_schema, cur_table, cur_dml_type})
			gtid_st[cur_gtid] = v
		}
	case 33:
		var Gtid struct {
			B      byte
			Uuid   [16]byte
			Seq_no uint64
		}
		Edata := bf.Next(data_length - 19)
		be := bytes.NewReader(Edata)
		err := binary.Read(be, binary.LittleEndian, &Gtid)
		if err != nil {
			fmt.Println("gtid binary.Read failed:", err)
			return
		}
		cur_gtid = fmt.Sprintf("%x:%d", Gtid.Uuid, Gtid.Seq_no)
		st_pos = int(next_pos) - data_length
		Pos[cur_gtid] = Pos_total{start_pos: st_pos}
		commit_tim[cur_gtid] = tim
	//	fmt.Println("tim:",tim )
	case 16:
		xid := [8]byte{}
		Edata := bf.Next(data_length - 19)
		be := bytes.NewReader(Edata)
		err := binary.Read(be, binary.LittleEndian, &xid)
		if err != nil {
			fmt.Println("xid binary.Read failed:", err)
			return
		}
		ed_pos = next_pos
		Exist_xid = true
		cur_schema = ""
		cur_table = ""
		cur_dml_type = ""
	default:
		_ = bf.Next(data_length - 19)
	}
	if Exist_xid {
		Pos[cur_gtid] = Pos_total{start_pos: st_pos,
			end_pos: ed_pos,
		}
		Exist_xid = false
	} else {
		Pos[cur_gtid] = Pos_total{start_pos: st_pos,
			end_pos: Last_que_pos,
		}
	}

}
func Ana_Map(txc map[string]Pos_total) {
	var st []string
	for k, _ := range txc {
		st = append(st, k)
	}
	sort.Strings(st)
	for _, v := range st {

		if v != "" {
			if len(gtid_st[v]) == 0 {
				fmt.Println(v, "[DDL]", "start_pos:", Pos[v].start_pos, "end_pos:", Pos[v].end_pos, "event_length", int(Pos[v].end_pos)-Pos[v].start_pos, " start_time:", time.Unix(int64(dt[v]), 0).Format("2006-01-02 15:04:05"), "commit_time:", time.Unix(int64(commit_tim[v]), 0).Format("2006-01-02 15:04:05"), "trx_exe_long:", commit_tim[v]-dt[v], "exe_time:", exetime[v])
			} else {
				fmt.Println(v, gtid_st[v], "start_pos:", Pos[v].start_pos, "end_pos:", Pos[v].end_pos, "event_length", int(Pos[v].end_pos)-Pos[v].start_pos, " start_time:", time.Unix(int64(dt[v]), 0).Format("2006-01-02 15:04:05"), "commit_time:", time.Unix(int64(commit_tim[v]), 0).Format("2006-01-02 15:04:05"), "trx_exe_long:", commit_tim[v]-dt[v], "exe_time:", exetime[v])

			}
		}

	}

}
