package triblab
import "fmt"
import "log"

const (
	GARBAGE_KVP = 90
	GARBAGE_LIST = 99
)

type Garbage struct {
	Backend	string
	Key		string
	Type	uint
}

func (self *Garbage) String() string {
	var _type string;
	if self.Type == GARBAGE_LIST { _type = "List" } else { _type = "KVP" }
	return fmt.Sprintf("Garbage for %s: %s of type %s", self.Backend, self.Key, _type)
}

type GarbageCollector struct {

}

func (self *GarbageCollector) mark(gb *Garbage) {
	// TODO
	log.Println(gb)
}
