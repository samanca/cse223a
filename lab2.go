package triblab

import (
	. "trib"
)

func NewBinClient(backs []string) BinStorage {
	BinS:=BinStorageWrapper{ back_ends: backs }
	BinS.bootStrapRing()
	BinS.fixPreviousPointer()
	go BinS.updateRing()
	return BinS
}

func ServeKeeper(kc *KeeperConfig) error {
	k := keeper{ config: kc }
	if kc.Ready != nil { kc.Ready <- true }
	return k.run()
}

func NewFront(s BinStorage) Server {
	return &TServer { storage: s}
}
