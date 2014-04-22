package triblab

import (
	. "trib"
)

func NewBinClient(backs []string) BinStorage {
	return BinStorageWrapper{ back_ends: backs }
}

func ServeKeeper(kc *KeeperConfig) error {
	k := keeper{ config: kc }
	if kc.Ready != nil { kc.Ready <- true }
	return k.run()
}

func NewFront(s BinStorage) Server {
	return &TServer { storage: s}
}

