package util

import (
	"log"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// from log
func Itoa(buf *[]byte, i int, wid int) {
	// Assemble decimal in reverse order.
	var b [20]byte
	bp := len(b) - 1
	for i >= 10 || wid > 1 {
		wid--
		q := i / 10
		b[bp] = byte('0' + i - q*10)
		bp--
		i = q
	}
	// i < 10
	b[bp] = byte('0' + i)
	*buf = append(*buf, b[bp:]...)
}

func GetTimeBuf() []byte {
	var buf []byte
	t := time.Now()
	_, min, sec := t.Clock()
	Itoa(&buf, min, 2)
	buf = append(buf, ':')
	Itoa(&buf, sec, 2)
	buf = append(buf, '.')
	Itoa(&buf, t.Nanosecond()/1e6, 3)
	return buf
}
