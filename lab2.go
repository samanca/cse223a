package triblab

import (
	. "trib"
)

func NewBinClient(backs []string) BinStorage {
	panic("todo")
}

func ServeKeeper(kc *KeeperConfig) error {
	k := keeper{ config: kc }
	if kc.Ready != nil { kc.Ready <- true }
	return k.run()
}

func NewFront(s BinStorage) Server {
	panic("todo")
}

