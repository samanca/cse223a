package triblab
import . "trib"
import "hash/crc32"

type BinStorageWrapper struct {
	back_ends[] string
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
