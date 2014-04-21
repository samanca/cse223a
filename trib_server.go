package triblab
import . "trib"
import "sync"
import "log"
import "fmt"

type TServer struct {
	lock  sync.Mutex
	storage BinStorage
}

var _ *TServer = new(TServer)

const (
	USERS = "USERS"
	POSTS = "POSTS"
)

// Utilities
func validateUsername(user string) error {
	if len(user) > MaxUsernameLen {
		return fmt.Errorf("username %q too long", user)
	}
	if !IsValidUsername(user) {
		return fmt.Errorf("invalid username %q", user)
	}
	return nil;
}

func inArray(needle string, haystack[] string) bool {
	for i := range haystack {
		if haystack[i] == needle { return true }
	}
	return false
}

func makeNS(user string, store string) string {
	return user + "::" + store
}

func makeFollowPair(who string, whom string) string {
	return makeNS(who, whom)
}

//
func (self *TServer) acquireBin(user string) Storage {
	// TODO optimize by maintaining a list of recent connections (persistent)
	return self.storage.Bin(user)
}

func (self *TServer) userList(users *List) error {
	b := self.acquireBin(USERS)
	e := b.ListGet(USERS, users)
	if e != nil { return e }
	return nil
}

func (self *TServer) userExists(user string) bool {
	// TODO optimize by first looking at local cache
	users := new(List)
	e := self.userList(users)
	if e != nil {
		log.Printf("Error while looking for user %s: %s\n", user, e)
		return true;
	}
	return inArray(user, users.L)
}

func (self *TServer) getUsers() []string {
	users := new(List)
	e := self.userList(users)
	if e != nil {
		log.Printf("Error while trying to get the list of users: %s\n", e)
		return nil
	}
	return users.L
}

// Service Interfaces
func (self TServer) SignUp(user string) error {

	err := validateUsername(user)
	if err != nil { return err }

	if self.userExists(user) {
		return fmt.Errorf("user %q already exists!", user)
	}

	var ok bool
	b := self.acquireBin(USERS)
	kv := KeyValue{ Key: USERS, Value: user }

	self.lock.Lock()
	defer self.lock.Unlock()
	e := b.ListAppend(&kv, &ok)

	if e == nil && !ok {
		return fmt.Errorf("failed creating new user!")
	}

	return e
}

func (self TServer) ListUsers() ([]string, error) {
	// TODO optimize by first looking at local cache
	return self.getUsers(), nil;
}

func (self TServer) Follow(who, whom string) error {

	t, e := self.IsFollowing(who, whom)

	if e != nil { return e }
	if t != false {
		return fmt.Errorf("%s is already following %s", who, whom)
	}

	b := self.acquireBin(who)

	following, err := self.Following(who)
	if err != nil {
		return err
	}

	if len(following) >= MaxFollowing {
		return fmt.Errorf("You have already reached the limit!")
	}

	var OK bool

	// TODO Store Clock
	e = b.Set(&KeyValue{ Key: makeFollowPair(who, whom), Value: "FOLLOWING" }, &OK)
	if OK != true { return fmt.Errorf("Unable to create new follower!") }

	return e
}

func (self TServer) IsFollowing(who, whom string) (bool, error) {

	if who == whom {
		return false, fmt.Errorf("You cannot become your follower!")
	}

	if !self.userExists(who) {
		return false, fmt.Errorf("Source user does not exist!")
	}

	if !self.userExists(whom) {
		return false, fmt.Errorf("Target user does not exist!")
	}

	b := self.acquireBin(who)

	var temp string
	e := b.Get(makeFollowPair(who, whom), &temp)
	if temp != "" && temp != "0" {
		return true, e
	} else {
		return false, e
	}
}

func (self TServer) Unfollow(who, whom string) error {

	if who == whom {
		return fmt.Errorf("cannot unfollow oneself!")
	}

	b, e := self.IsFollowing(who, whom)

	if e != nil {
		return e
	}

	if b != true {
		return fmt.Errorf("user %q is not following %q", who, whom)
	}

	bin := self.acquireBin(who)
	var OK bool
	e = bin.Set(&KeyValue{ Key: makeFollowPair(who, whom), Value: "" }, &OK)

	if OK != true {
		return fmt.Errorf("failed while %s doing unflow for %s", who, whom)
	}

	return e
}

func (self TServer) Following(who string) ([]string, error) {

	if self.userExists(who) != true {
		return nil, fmt.Errorf("user %s does not exist!", who)
	}

	b := self.acquireBin(who)
	var list List
	var p Pattern
	p.Prefix = makeFollowPair(who,"")
	e := b.Keys(&p, &list)

	if e != nil {
		return make([]string, 0), e
	}

	var r []string
	for i := range list.L {
		r = append(r, list.L[i])
	}

	return r, nil
}

func (self TServer) Post(user, post string, c uint64) error {
	/*
	if len(post) > trib.MaxTribLen {
		return fmt.Errorf("trib too long")
	}

	self.lock.Lock()
	defer self.lock.Unlock()

	u, e := self.findUser(user)
	if e != nil {
		return e
	}

	if self.seq < c {
		self.seq = c
	}
	self.seq++
	if self.seq == math.MaxUint64 {
		panic("run out of seq number")
	}

	t := time.Now()
	u.post(user, post, self.seq, t)

	return nil
	*/
	return nil
}

func (self TServer) Home(user string) ([]*Trib, error) {
	/*
	self.lock.Lock()
	defer self.lock.Unlock()

	u, e := self.findUser(user)
	if e != nil {
		return nil, e
	}

	return u.listHome(), nil
	*/
	return nil, nil
}

func (self TServer) Tribs(user string) ([]*Trib, error) {
	/*
	self.lock.Lock()
	defer self.lock.Unlock()

	u, e := self.findUser(user)
	if e != nil {
		return nil, e
	}

	return u.listTribs(), nil
	*/
	return nil, nil
}
