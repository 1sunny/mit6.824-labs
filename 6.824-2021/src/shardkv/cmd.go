package shardkv

import (
	"fmt"
)

type Cmd struct {
	OpType CmdType
	Data   interface{}
}

func (command Cmd) String() string {
	return fmt.Sprintf("{Type:%v,Data:%+v}", command.OpType, command.Data)
}

type CmdType string

const (
	Operation = "Operation"
	Config    = "Config"
	Insert    = "Insert"
	Delete    = "Delete"
)
