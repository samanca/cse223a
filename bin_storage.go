package triblab

import . "trib"
import "hash/crc32"
import "time"
import "fmt"
import "log"

type BinStorageWrapper struct {
    back_ends []string
    chord     Chord1
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

func (self *BinStorageWrapper) Bin(name string) Storage {

	if LogEnabled { log.Print(self.chord.ring) }

    var cli *client
    var ip string

	id := self.chord.getHash1(name) // hash value for name
    _, succ_ip := self.chord.find_succ(id) // it should be the owner

	if succ_ip == EMPTY_STRING {
        log.Printf("succ_ip == EMPTY")
        return nil
    }

    cli = &client{addr: succ_ip}
    c1, err := cli.acquireConnection() // test if the guy is up
    if err == nil {
        ip = succ_ip
		c1.Close()
    } else {
		if LogEnabled { log.Printf("trying to find another successor ...") }

		succ_ip_val := self.chord.getHash1(succ_ip) + 1
		_, succ_ip := self.chord.find_succ(succ_ip_val) // the successor should be able to respond

		if succ_ip == EMPTY_STRING{
			log.Printf("succ_ip is empty string in Bin(%s)", name)
		}

		cli = &client{addr: succ_ip}
		c2, err := cli.acquireConnection() // test if the guy is up

		if err == nil {
			ip = succ_ip
			c2.Close()
		} else {
			log.Printf("DANGER: two consecutive nodes are down!!!!")
			return nil
		}
    }

    final_cli := &OpLogClient{addr: ip, ns: name}
    final_cli.init()

    log.Printf("Connected to %s", ip)

    return final_cli
}

func (self *BinStorageWrapper) bootStrapRing() {
    var cli *client
    if LogEnabled {
        log.Print("Entered the ring")
    }
    for i := 0; i < len(self.back_ends); i++ {
        cli = &client{addr: self.back_ends[i]}
        _, err := cli.acquireConnection()
        if err == nil {
            self.chord.addNode(self.back_ends[i])
        }
    }
    if LogEnabled {
        log.Print("Exit the ring")
        log.Print("Ring Size")
        log.Print(len(self.chord.ring))
    }
}

func (self *BinStorageWrapper) fixPreviousPointer() {
    var prev string
    var prev_val uint32
    var cli *client
    if LogEnabled {
        log.Print("Entered the fixPrevious")
    }
    for i := range self.chord.ring {
        cli = &client{addr: self.chord.ring[i].ip}
        _, err := cli.acquireConnection()
        if err == nil {
            err1 := cli.Get("PREV", &prev)
            if err1 != nil {
                fmt.Errorf("Error with Get PREV")
                //log.Print("Error while Get PREV")
            } else {
                if LogEnabled {
                    log.Print("PREV-", prev)
                }
                prev_val = self.chord.getHash1(prev)
                self.chord.ring[i].prev_ip = prev
                self.chord.ring[i].prev = prev_val
            }
        }
        if LogEnabled {
            log.Print("Exit fixPrevious")
        }
    }
}

func (self *BinStorageWrapper) updateRing() {
    var cli *client
    var next string
    var prev string
    var next_val uint32
    var prev_val uint32
    //	var name string
    var incr uint32
    var ip string
    incr = 0
    var found bool
    status := 0
    if LogEnabled {
        log.Print("Entering updateRing")
    }

    for {
        // Run every 15 seconds
        if LogEnabled {
            log.Print("Running updateRing for:")
            log.Print(incr)
        }
        incr = incr + 1
        time.Sleep(15 * time.Second)
        if LogEnabled {
            log.Print(len(self.chord.ring))
        }

        for i := range self.chord.ring {
            log.Print("Value of i and size of ring just before crashing - ", i, "and ", len(self.chord.ring))
            self.printRing()
            ip = self.chord.ring[i].ip

            cli = &client{addr: ip}

            _, err := cli.acquireConnection()
            if err == nil { // Node is alive
                // Read PREV and NEXT from the live node
                err1 := cli.Get("NEXT", &next)
                err2 := cli.Get("PREV", &prev)

                if err1 != nil {
                    fmt.Errorf("Error with Get NEXT")
                    //log.Print("Error while getting Next")
                }
                if err2 != nil {
                    fmt.Errorf("Error with Get PREV")
                    //log.Print("Error while getting PREV")
                }

                next_val = self.chord.getHash1(next)
                prev_val = self.chord.getHash1(prev)

                if LogEnabled {
                    log.Print("Got next and pre values")
                    log.Print(self.chord.ring[i].next)
                    log.Print(next_val)
                }

                if self.chord.ring[i].next != next_val || self.chord.ring[i].prev != prev_val {
                    // New node was added or some node was deleted
                    found = false
                    for j := range self.chord.ring {
                        if self.chord.ring[j].ip == next {
                            found = true
                            status = 1
                            break
                        }
                    }

                    if found == false {
                        status = 2
                    }
                }
            } else { // if the connection was not successful then remove that node
                status = 3
                //self.chord.removeNode(ip)
            }
        }

        if status == 1 {
            self.chord.removeNode(next) // This should remove the node as well as the fix succ and prev
        } else if status == 2 {
            self.chord.addNode(next)
        } else if status == 3 { // if the connection was not successful then remove that node
            self.chord.removeNode(ip)
        }

    }   //END OF update timer
    self.printRing()
    if LogEnabled {
        log.Print("Leaving updateRing")
    }
}

func (self *BinStorageWrapper) printRing() {
    log.Print("Print the ring")
    log.Print("Size of the ring PrintLog")
    log.Print(len(self.chord.ring))
    for i := range self.chord.ring {
        fmt.Printf("%d--%d--%d--%s--%s\n", self.chord.ring[i].hash, self.chord.ring[i].prev, self.chord.ring[i].next, self.chord.ring[i].ip, self.chord.ring[i].succ_ip)
    }
    log.Print("Getting out of the Printring")
}

func (self *BinStorageWrapper) testFindSucc() {
    ls := []uint32{562156532, 653006829, 674734111, 825012062, 947739488, 949674805, 1058053961, 1073162028, 1177263824, 1209511639, 1223694018, 1333422798}

    for i := range ls {
        succ, succ_ip := self.chord.find_succ(ls[i])
        log.Print("SUCC--", succ, "--", succ_ip)
    }

}
