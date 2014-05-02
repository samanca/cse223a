package triblab
import . "trib"
import "net/rpc"
//import "fmt"
import "strings"

type OpLogClient struct {
	addr string
	ns string
}

const (
	LOG_KEY = "OPLOG"

	OP_SET = 0 // opCode for Get()
	OP_LIST_APPEND = 1 // opCode for ListAppend()
	OP_LIST_REMOVE = 2 // opCode for ListRemove()
)

/*
 * Our OpLog will be a list of encoded OpLogEntries, ordered by Primary Time
 */
type OpLogEntry struct {
	opCode uint // OP_SET | OP_LIST_APPEND | OP_LIST_REMOVE
	data KeyValue
}

func (self *client) acquireConnection() (*rpc.Client, error) {
	return rpc.Dial("tcp", self.addr);
}

func (self *client) makeNS(key string) string {
	if (self.ns == "") {
		return key
	} else {
		return self.ns + "::" + key
	}
}

func removeNS(entry string) string {
	t := strings.Split(entry, "::")
	if len(t) == 2 {
		return t[1]
	} else {
		return t[0]
	}
}

func extractNS(entry string) string {
	t := strings.Split(entry, "::")
	if len(t) == 2 {
		return t[0]
	} else {
		return t[0] // EMPTY_STRING?!
	}
}

/*
 * Implementing KeyString
 */

func (self *client) Set(kv *KeyValue, succ *bool) error {
	c, err := self.acquireConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		op := &OpLogEntry{opCode:OP_SET,data:kv2}
		op_j, e := json.Marshal(op)
		if e != nil {
		return fmt.Errorf("error while marshaling the OP Code")
		}
		OPkv := KeyValue{ Key:LOG_KEY , Value: op_j }
		err = c.Call("Storage.ListAppend", &OPkv, succ);

		c.Close()
	}
	return err;
}


func (self *client) ListAppend(kv *KeyValue, succ *bool) error {
	c, err := self.acquireConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		op := &OpLogEntry{opCode:OP_SET,data:kv2}
		op_j, e := json.Marshal(op)
		if e != nil {
		return fmt.Errorf("error while marshaling the OP Code")
		}
		OPkv := KeyValue{ Key:LOG_KEY , Value: op_j }
		err = c.Call("Storage.ListAppend", &OPkv, succ);
		c.Close()
	}
	return err;
}

func (self *client) ListRemove(kv *KeyValue, n *int) error {
	c, err := self.acquireConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		op := &OpLogEntry{opCode:OP_SET,data:kv2}
		op_j, e := json.Marshal(op)
		if e != nil {
		return fmt.Errorf("error while marshaling the OP Code")
		}
		OPkv := KeyValue{ Key:LOG_KEY , Value: op_j }
		err = c.Call("Storage.ListAppend", &OPkv, succ);
		c.Close()
	}
	return err;
}


/*
 * Implementing Storage
 */
func (self *client) Clock(atLeast uint64, ret *uint64) error {
	c, err := self.acquireConnection();
	if err == nil {
		err = c.Call("Storage.Clock", atLeast, ret);
		c.Close()
	}
	return err;
}

var _ Storage = new(client)
