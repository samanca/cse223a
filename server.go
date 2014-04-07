package triblab

import . "trib"
import "fmt"
import "net"
import "net/rpc"

type server struct { }

func (self *server) init(b *BackConfig) error {

	//fmt.Printf("Starting server ...\n");

	err := rpc.Register(b.Store);
	if err != nil {
		b.Ready <- false;
		return err;
	}
	//fmt.Printf("RPC registered!\n");

	rpc.HandleHTTP();
	//fmt.Printf("Handle HTTP OK!\n");

	l, e := net.Listen("tcp", b.Addr);
	if e != nil {
		b.Ready <- false;
		return err;
	}
	//fmt.Printf("Listening on port ...\n");

	//fmt.Printf("Ready to serve ...\n");
	if b.Ready != nil {
		b.Ready <- true;
	}

	for {
		if conn, err := l.Accept(); err != nil {
			fmt.Printf("Failed accepting connection: " + err.Error() + "\n");
		} else {
			//fmt.Printf("New connection accepted!\n")
			go rpc.ServeConn(conn)
		}
	}

	return nil;
}


