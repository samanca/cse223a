package triblab
import . "trib"
//import "fmt"
import "net/rpc"

type client struct {

	addr string
	handler *rpc.Client
	connected bool

}

/*
func close(c *client) {
	if c.connected {
		c.handler.close();
	}
}
*/

func (self *client) CheckConnection() error {
	//if !self.connected {
		c, err := rpc.Dial("tcp", self.addr);
		self.handler = c;
		return err;
	//}
	//self.connected = true;
	//return nil;
}

/*
 * Implementing KeyString
 */
func (self *client) Get(key string, value *string) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.Get", key, value);
		/*
		if err == ErrShutdown {
			self.connected = false;
			err = Get(key, value);
		}
		*/
	}
	return err;
}

func (self *client) Set(kv *KeyValue, succ *bool) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.Set", kv, succ);
	}
	return err;
}

func (self *client) Keys(p *Pattern, list *List) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.Keys", p, list);
	}
	return err;
}

/*
 * Implementing KeyList
 */
func (self *client) ListGet(key string, list *List) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.ListGet", key, list);
	}
	return err;
}

func (self *client) ListAppend(kv *KeyValue, succ *bool) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.ListAppend", kv, succ);
	}
	return err;
}

func (self *client) ListRemove(kv *KeyValue, n *int) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.ListRemove", kv, n);
	}
	return err;
}

func (self *client) ListKeys(p *Pattern, list *List) error {
	err := self.CheckConnection();
	if err == nil {
		err = self.handler.Call("Storage.ListKeys", p, list);
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
	}
	return err;
}

var _ Storage = new(client)
