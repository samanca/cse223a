package triblab
import . "trib"
import "net/rpc"
import "encoding/json"
import "fmt"
//import "strings"

type OpLogClient struct {
	addr string
	ns string
}

func (self *OpLogClient) acquireConnection() (*rpc.Client, error) {
	return rpc.Dial("tcp", self.addr);
}

func (self *OpLogClient) makeNS(key string) string {
	if (self.ns == "") {
		return key
	} else {
		return self.ns + "::" + key
	}
}

/*
 * Implementing KeyString
 */

func (self *OpLogClient) Set(kv *KeyValue, succ *bool) error {
	c, err := self.acquireConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		op := &OpLogEntry{opCode:OP_SET,data:kv2}
		op_j, e := json.Marshal(op)
		if e != nil {
		return fmt.Errorf("error while marshaling the OP Code")
		}
		OPkv := KeyValue{ Key:LOG_KEY , Value: string(op_j) }
		err = c.Call("Storage.ListAppend", &OPkv, succ);

		c.Close()
	}
	return err;
}


func (self *OpLogClient) ListAppend(kv *KeyValue, succ *bool) error {
	c, err := self.acquireConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		op := &OpLogEntry{opCode:OP_SET,data:kv2}
		op_j, e := json.Marshal(op)
		if e != nil {
		return fmt.Errorf("error while marshaling the OP Code")
		}
		OPkv := KeyValue{ Key:LOG_KEY , Value: string(op_j) }
		err = c.Call("Storage.ListAppend", &OPkv, succ);
		c.Close()
	}
	return err;
}

func (self *OpLogClient) ListRemove(kv *KeyValue, n *int) error {
	var succ bool
	c, err := self.acquireConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		op := &OpLogEntry{opCode:OP_SET,data:kv2}
		op_j, e := json.Marshal(op)
		if e != nil {
		return fmt.Errorf("error while marshaling the OP Code")
		}
		OPkv := KeyValue{ Key:LOG_KEY , Value: string(op_j) }
		err = c.Call("Storage.ListAppend", &OPkv, succ);
		c.Close()
	}
	return err;
}


/*
 * Implementing Storage
 */
func (self *OpLogClient) Clock(atLeast uint64, ret *uint64) error {
	c, err := self.acquireConnection();
	if err == nil {
		err = c.Call("Storage.Clock", atLeast, ret);
		c.Close()
	}
	return err;
}

var _ Storage = new(client)
