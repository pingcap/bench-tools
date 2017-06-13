package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

const KVDataFilePfx = "kvdata"
const KeyPfx uint8 = 'k'
const IdxPfx uint8 = 'i'

type Config struct {
	TableID   uint64
	Count     uint64
	ValLen    []int // minValLen, maxValLen
	IdxCnt    int
	IdxValLen [][]int
}

func (c *Config) show() {
	log.Printf("Config: {TableID=%d, Count=%d, ValLen=%x, IdxCnt=%d, IdxValLen=%x}",
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

func GenDataKey(buf []byte, tid uint64, rid uint64) []byte {
	bytesBuffer := bytes.NewBuffer(buf)
	binary.Write(bytesBuffer, binary.BigEndian, KeyPfx)
	binary.Write(bytesBuffer, binary.BigEndian, tid)
	binary.Write(bytesBuffer, binary.BigEndian, rid)
	return bytesBuffer.Bytes()
}

func GenIdxKey(buf []byte, tid uint64, val []byte, rid uint64) (int, []byte) {
	keyLen := 1 + 2*binary.Size(uint64(0)) + len(val)
	byteBuffer := bytes.NewBuffer(buf)
	binary.Write(byteBuffer, binary.BigEndian, IdxPfx)
	binary.Write(byteBuffer, binary.BigEndian, tid)
	binary.Write(byteBuffer, binary.BigEndian, val)
	binary.Write(byteBuffer, binary.BigEndian, rid)
	return keyLen, byteBuffer.Bytes()
}

func GenKVData(cfg string) {
	conf := ParseConf(cfg)
	ofile := fmt.Sprintf("%s.%s", KVDataFilePfx, strings.TrimSuffix(cfg, ".toml"))
	log.Printf("generate kv data, output=%s count=%d vallen=%d", ofile, conf.Count, conf.ValLen)
	f, err := os.Create(ofile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// serialize conf to write
	binary.Write(f, binary.BigEndian, &conf)

	// init buffer
	keyLen := 1 + 2*binary.Size(uint64(0))
	keyBuf := make([]byte, keyLen)
	valBuf := make([]byte, conf.ValLen[1])
	idxKeyBufs := make([][]byte, conf.IdxCnt)
	idxVals := make([][]byte, conf.IdxCnt)
	for i := 0; i < conf.IdxCnt; i++ {
		idxKeyLen := 1 + 2*binary.Size(uint64(0)) + conf.IdxValLen[i][1]
		idxKeyBufs[i] = make([]byte, idxKeyLen)
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
		rand.Read(valBuf)
		f.Write(valBuf[0:valLen])

		// put index keylen, key
		for i := 0; i < conf.IdxCnt; i++ {
			valLen := RandLenInRange(conf.IdxValLen[i][0], conf.IdxValLen[i][1])
			rand.Read(idxVals[i])
			keyLen, key := GenIdxKey(idxKeyBufs[i], conf.TableID, idxVals[i][:valLen], rid)
			binary.Write(f, binary.BigEndian, uint64(keyLen))
			f.Write(key)
		}
	}
}

func DumpKVData(file string, cnt uint64, offset uint64) {
	log.Printf("dump kv data, file=%s count=%d offset=%d", file, cnt, offset)
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// read Config info
	var conf Config
	binary.Read(f, binary.BigEndian, &conf)
	conf.show()
	// check count
	if cnt > conf.Count {
		log.Fatalln("read too much record")
		os.Exit(-1)
	}

	keyLen := 1 + 2*binary.Size(uint64(0))
	keyBuf := make([]byte, keyLen)
	var valLen uint64
	valBuf := make([]byte, conf.ValLen[1])
	idxKeyBufs := make([][]byte, conf.IdxCnt)
	for i := 0; i < conf.IdxCnt; i++ {
		idxKeyLen := 1 + 2*binary.Size(uint64(0)) + conf.IdxValLen[i][1]
		idxKeyBufs[i] = make([]byte, idxKeyLen)
	}

	var i uint64
	for i = 0; i < cnt; i++ {
		f.Read(keyBuf)
		binary.Read(f, binary.BigEndian, &valLen)
		f.Read(valBuf[:valLen])
		log.Printf("data: %x -> %x\n", keyBuf, valBuf[:valLen])

		for i := 0; i < conf.IdxCnt; i++ {
			var idxKeyLen uint64
			binary.Read(f, binary.BigEndian, idxKeyLen)
			binary.Read(f, binary.BigEndian, idxKeyBufs[i][:idxKeyLen])
			log.Printf("index: %x", idxKeyBufs[i])
		}
	}
}

func main() {
	cmd := flag.String("cmd", "generate", "sub command [generate|dump]")
	cfg := flag.String("config", "gen.toml", "config file for generate")
	src := flag.String("src", "", "data file for dump")
	count := flag.Uint64("count", 100000, "kv record count for dump")
	offset := flag.Uint64("offset", 0, "offset of kv record offset for dump")
	flag.Parse()

	switch *cmd {
	case "generate":
		GenKVData(*cfg)
	case "dump":
		DumpKVData(*src, *count, *offset)
	default:
		panic("invalid command")
	}
}
