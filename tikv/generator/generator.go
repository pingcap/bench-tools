package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/ngaut/log"
	msgpack "gopkg.in/vmihailenco/msgpack.v2"
)

const KVDataFilePfx = "kvdata"
const KeyPfx uint8 = 'k'
const IdxPfx uint8 = 'i'

type Config struct {
	TableID   uint64
	Count     uint64
	ValLen    [2]int // minValLen, maxValLen
	IdxCnt    int
	IdxValLen [][2]int
}

func (c *Config) show() {
	log.Infof("Config: {TableID=%d, Count=%d, ValLen=%d, IdxCnt=%d, IdxValLen=%d}",
		c.TableID, c.Count, c.ValLen, c.IdxCnt, c.IdxValLen)
}

func ParseConf(cfg string) Config {
	f, err := os.Open(cfg)
	if err != nil {
		panic(err)
	}
	src, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	var conf Config
	toml.Decode(string(src), &conf)
	return conf
}

func RandLenInRange(minLen int, maxLen int) int {
	if minLen == maxLen {
		return minLen
	}
	diff := rand.Int31n(int32(maxLen - minLen))
	return minLen + int(diff)
}

func GetFilePos(f *os.File) int64 {
	pos, err := f.Seek(0, 1)
	if err != nil {
		panic(err)
	}
	return pos
}

func GenDataKey(buf *bytes.Buffer, tid uint64, rid uint64) []byte {
	buf.Reset()
	binary.Write(buf, binary.BigEndian, KeyPfx)
	binary.Write(buf, binary.BigEndian, tid)
	binary.Write(buf, binary.BigEndian, rid)
	return buf.Bytes()
}

func GenIdxKey(buf *bytes.Buffer, tid uint64, val []byte, rid uint64) (int, []byte) {
	buf.Reset()
	keyLen := 1 + 2*binary.Size(uint64(0)) + len(val)
	binary.Write(buf, binary.BigEndian, IdxPfx)
	binary.Write(buf, binary.BigEndian, tid)
	binary.Write(buf, binary.BigEndian, val)
	binary.Write(buf, binary.BigEndian, rid)
	return keyLen, buf.Bytes()
}

func GenKVData(cfg string) {
	conf := ParseConf(cfg)
	conf.show()
	ofile := fmt.Sprintf("%s.%s", KVDataFilePfx, strings.TrimSuffix(cfg, ".toml"))
	log.Infof("generate kv data, output=%s count=%d vallen=%d", ofile, conf.Count, conf.ValLen)
	f, err := os.Create(ofile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// serialize conf to write
	confBuf, err := msgpack.Marshal(conf)
	if err != nil {
		panic(err)
	}
	confLen := len(confBuf)
	err = binary.Write(f, binary.BigEndian, uint64(confLen))
	if err != nil {
		panic(err)
	}
	_, err = f.Write(confBuf)
	if err != nil {
		panic(err)
	}
	log.Debugf("File conf pos=%d", GetFilePos(f))

	// init key buf and val buf
	keyLen := binary.Size(KeyPfx) + 2*binary.Size(uint64(0))
	keyBuf := bytes.NewBuffer([]byte{})
	keyBuf.Grow(keyLen)
	valBuf := make([]byte, conf.ValLen[1])
	// init index key buf
	idxKeyBufs := make([]*bytes.Buffer, conf.IdxCnt)
	idxVals := make([][]byte, conf.IdxCnt)
	for i := 0; i < conf.IdxCnt; i++ {
		idxKeyLen := binary.Size(IdxPfx) + 2*binary.Size(uint64(0)) + conf.IdxValLen[i][1]
		idxKeyBufs[i] = bytes.NewBuffer([]byte{})
		idxKeyBufs[i].Grow(idxKeyLen)
		idxVals[i] = make([]byte, conf.IdxValLen[i][1])
	}
	var rid uint64
	for rid = 0; rid < conf.Count; rid++ {
		// put key
		key := GenDataKey(keyBuf, conf.TableID, rid)
		f.Write(key)
		// put vallen, val
		valLen := RandLenInRange(conf.ValLen[0], conf.ValLen[1])
		binary.Write(f, binary.BigEndian, uint64(valLen))
		rand.Read(valBuf[:valLen])
		f.Write(valBuf[:valLen])
		log.Debugf("Put key=%d vallen=%d val=%d", key, valLen, valBuf[:valLen])

		// put index keylen, key
		for i := 0; i < conf.IdxCnt; i++ {
			valLen := RandLenInRange(conf.IdxValLen[i][0], conf.IdxValLen[i][1])
			rand.Read(idxVals[i][:valLen])
			keyLen, key := GenIdxKey(idxKeyBufs[i], conf.TableID, idxVals[i][:valLen], rid)
			binary.Write(f, binary.BigEndian, uint64(keyLen))
			f.Write(key)
			log.Debugf("put Idx%d tid=%d rid=%d vallen=%d key=%d",
				i, conf.TableID, rid, valLen, key)
		}
	}
}

func DumpKVData(file string, cnt uint64, offset uint64) {
	log.Infof("dump kv data, file=%s count=%d offset=%d", file, cnt, offset)
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// read Config info
	var confLen uint64
	binary.Read(f, binary.BigEndian, &confLen)
	confBuf := make([]byte, confLen)
	//var readLen int
	_, err = f.Read(confBuf)
	if err != nil {
		panic(err)
	}
	var conf Config
	err = msgpack.Unmarshal(confBuf, &conf)
	if err != nil {
		panic(err)
	}
	conf.show()
	log.Debugf("File conf pos=%d", GetFilePos(f))

	// check count
	if cnt > conf.Count {
		log.Fatal("read too much record")
	}

	keyLen := binary.Size(KeyPfx) + 2*binary.Size(uint64(0))
	keyBuf := make([]byte, keyLen)
	var valLen uint64
	valBuf := make([]byte, conf.ValLen[1])
	idxKeyBufs := make([][]byte, conf.IdxCnt)
	for i := 0; i < conf.IdxCnt; i++ {
		idxKeyLen := binary.Size(KeyPfx) + 2*binary.Size(uint64(0)) + conf.IdxValLen[i][1]
		idxKeyBufs[i] = make([]byte, idxKeyLen)
	}

	var i uint64
	for i = 0; i < cnt; i++ {
		f.Read(keyBuf)
		binary.Read(f, binary.BigEndian, &valLen)
		f.Read(valBuf[:valLen])
		log.Debugf("Read key=%d vallen=%d val=%d", keyBuf, valLen, valBuf[:valLen])

		for i := 0; i < conf.IdxCnt; i++ {
			var idxKeyLen uint64
			binary.Read(f, binary.BigEndian, &idxKeyLen)
			binary.Read(f, binary.BigEndian, idxKeyBufs[i][:idxKeyLen])
			log.Debugf("Read idx%d key=%d", i, idxKeyBufs[i])
		}
	}
}

func main() {
	cmd := flag.String("cmd", "generate", "sub command [generate|dump]")
	cfg := flag.String("config", "example.toml", "config file for generate")
	src := flag.String("src", KVDataFilePfx+".example", "data file for dump")
	cnt := flag.Uint64("cnt", 1, "kv record count for dump")
	offset := flag.Uint64("offset", 0, "offset of kv record offset for dump")
	flag.Parse()

	switch *cmd {
	case "generate":
		GenKVData(*cfg)
	case "dump":
		DumpKVData(*src, *cnt, *offset)
	default:
		panic("invalid command")
	}
}
