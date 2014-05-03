package triblab
import . "trib"
import "time"
import "fmt"
import "trib/store"

type CoKeeper struct {
	config *KeeperConfig
	_store Storage
}

func (self *CoKeeper) init() error {
	self._store = store.NewStorage()
	back := &BackConfig{
		Addr:  self.config.Addrs[self.config.This],
		Store: self._store,
		Ready: make(chan bool, 1),
	}

	e := ServeBack(back)
	if e != nil { return e }
	return nil
}

func (self *CoKeeper) run(ch *chan bool) {

	myAddress := self.config.Addrs[self.config.This]
	var maxObservedAddress string = myAddress

	for {
		// rest for a while
		time.Sleep(1 * time.Second)

		// Pull everybody


		// decide about the future!
		if myAddress > maxObservedAddress {
			break
		}
	}

	*ch <- true // I am the PRIMARY from now on
}

/*
 * Call this method inside PRIMARY keeper for every
 * update that you make on your local Chord
 */
func (self *CoKeeper) UpdateChord(chord []byte) error {
	var success bool
	kv := KeyValue{ Key: "CHORD", Value: string(chord) }
	er := self._store.Set(&kv, &success)
	if !success {
		return fmt.Errorf("unable to read chord")
	}
	return er
}

/*
 * Call this method immediately after becoming primary
 * and inside the main function of keeper
 */
func (self *CoKeeper) GetMostUpdatedChord() ([]byte, error) {
	var value string
	err := self._store.Get("CHORD", &value)
	return []byte(value), err
}
