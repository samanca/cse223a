package triblab
import . "trib"
import "hash/crc32"
import "time"

type BinStorageWrapper struct {
	back_ends[] string
	chord  Chord1
}

func getHash(name string) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(name))
	return h.Sum32()
}

func (self *BinStorageWrapper) Map(name string) uint32 {
	h := getHash(name)
	c := uint32(len(self.back_ends))
	return h % c
}

func (self BinStorageWrapper) Bin(name string) Storage {
	server := self.back_ends[self.Map(name)]
	cli := &OpLogClient{ addr: server, ns: name }
	cli.init()
	return cli
}


func (self BinStorageWrapper) bootStrapRing(){
	 var cli *client
	 var name string
    for i:=0;i<len(self.back_ends);i++{
    	cli=&client{ addr: self.back_ends[i] }
    	c, err := cli.acquireConnection();
    	if err==nil{
    		self.chord.addNode(self.back_ends[i])
    	}
    }
}

/**
func (self BinStorageWrapper) query(){
	var cli *client
	for {
		// Run every 15 seconds
		time.Sleep(15 * time.Second)
		for i:= range self.chord.ring{
			cli=&client{ addr: self.back_ends[i] }
			c,err=cli.acquireConnection();

		}
}**/
