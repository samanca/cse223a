package triblab
import . "trib"
import "net/rpc"
//import "fmt"
import "strings"

type client struct {

	addr string
	handler *rpc.Client
	connected bool
	ns string

}

func (self *client) CheckConnection() error {
	if !self.connected {
		c, err := rpc.Dial("tcp", self.addr);
		self.handler = c;
		return err;
	}
	self.connected = true;
	return nil;
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

/*
 * Implementing KeyString
 */
func (self *client) Get(key string, value *string) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.Get", self.makeNS(key), value);
		if err == rpc.ErrShutdown {
			self.connected = false;
			err = self.Get(key, value);
		}
	}
	return err;
}

func (self *client) Set(kv *KeyValue, succ *bool) error {
	err := self.CheckConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		err = self.handler.Call("Storage.Set", &kv2, succ);
		if err == rpc.ErrShutdown {
			self.connected = false;
			err = self.Set(kv, succ);
		}
	}
	return err;
}

func (self *client) Keys(p *Pattern, list *List) error {
	err := self.CheckConnection();

	if list == nil {
		list = new(List);
	}

	if err == nil {
		p2 := Pattern{ Prefix: self.makeNS(p.Prefix), Suffix: p.Suffix }
		err = self.handler.Call("Storage.Keys", &p2, list);
		if err == rpc.ErrShutdown {
			self.connected = false;
			err = self.Keys(p, list);
		}
	}

	if list == nil {
		list = new(List)
	}

	if list.L == nil {
		list.L = make([]string, 0)
	} else {
		for i := range list.L {
			list.L[i] = removeNS(list.L[i])
		}
	}

	return err;
}

/*
 * Implementing KeyList
 */
func (self *client) ListGet(key string, list *List) error {
	err := self.CheckConnection();

	list.L = nil
	if err == nil {
		err = self.handler.Call("Storage.ListGet", self.makeNS(key), list);
		if err == rpc.ErrShutdown {
			self.connected = false;
			err = self.ListGet(key, list);
		}
	}

	if list.L == nil {
		list.L = make([]string, 0)
	}

	return err;
}

func (self *client) ListAppend(kv *KeyValue, succ *bool) error {
	err := self.CheckConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		err = self.handler.Call("Storage.ListAppend", &kv2, succ);
		if err == rpc.ErrShutdown {
			self.connected = false;
			err = self.ListAppend(kv, succ);
		}
	}
	return err;
}

func (self *client) ListRemove(kv *KeyValue, n *int) error {
	err := self.CheckConnection();
	if err == nil {
		kv2 := KeyValue{ Key: self.makeNS(kv.Key), Value: kv.Value }
		err = self.handler.Call("Storage.ListRemove", &kv2, n);
		if err == rpc.ErrShutdown {
			self.connected = false;
			err = self.ListRemove(kv, n);
		}
	}
	return err;
}

func (self *client) ListKeys(p *Pattern, list *List) error {
	err := self.CheckConnection();

	list.L = nil
	if err == nil {
		p2 := Pattern{ Prefix: self.makeNS(p.Prefix), Suffix: p.Suffix }
		err = self.handler.Call("Storage.ListKeys", &p2, list);
		if err == rpc.ErrShutdown {
			self.connected = false;
			err = self.ListKeys(p, list);
		}
	}

	if list.L == nil {
		list.L = make([]string, 0)
	} else {
		for i := range list.L {
			list.L[i] = removeNS(list.L[i])
		}
	}

	return err;
}

/*
 * Implementing Storage
 */
func (self *client) Clock(atLeast uint64, ret *uint64) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.Clock", atLeast, ret);
		if err == rpc.ErrShutdown {
			self.connected = false;
			err = self.Clock(atLeast, ret);
		}
	}
	return err;
}

var _ Storage = new(client)
