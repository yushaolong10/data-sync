package util

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/json-iterator/go"
	"github.com/satori/go.uuid"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func GetInternalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				strip := ipnet.IP.String()
				if strings.HasPrefix(strip, "10.") || strings.HasPrefix(strip, "172.16.") {
					return strip, nil
				}
			}
		}
	}

	return "", errors.New("no internal ip found")
}

func Hostname() (string, error) {
	hostname, err := os.Hostname()
	return hostname, err
}

func IdcName() string {
	hostName, err := Hostname()
	if err != nil {
		return "default"
	} else {
		strList := strings.Split(hostName, ".")
		if len(strList) == 5 {
			return strList[2]
		} else {
			return "default"
		}

	}

}

func Md5(str string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(str)))
}

func Sha1(args ...string) string {
	var buffer bytes.Buffer
	for _, v := range args {
		buffer.WriteString(v)
	}
	h := sha1.New()
	h.Write(buffer.Bytes())
	return fmt.Sprintf("%x", h.Sum(nil))
}

func GetCurrTime() int64 {
	return time.Now().Unix()
}

func GetCurrTimeMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func GetCurrTimeMicors() int64 {
	return time.Now().UnixNano() / int64(time.Microsecond)
}

func GetCurrTimeNs() int64 {
	return time.Now().UnixNano()
}

//获取毫秒间隔
func GetDurationMillis(begin time.Time) int64 {
	return time.Since(begin).Nanoseconds() / 1e6
}

//获取微秒间隔
func GetDurationMicros(begin time.Time) int64 {
	return time.Since(begin).Nanoseconds() / 1000
}

func StructToJson(data interface{}) string {
	if result, err := json.Marshal(data); err != nil {
		return ""
	} else {
		return string(result)
	}
}

func StringArrayHas(data []string, needle string) bool {
	for _, item := range data {
		if item == needle {
			return true
		}
	}
	return false
}

func StringArrayIntersect(src []string, dst []string) []string {
	srcMp := make(map[string]struct{})
	for _, v := range src {
		srcMp[v] = struct{}{}
	}
	ret := make([]string, 0)
	for _, v := range dst {
		if _, ok := srcMp[v]; ok {
			ret = append(ret, v)
		}
	}
	return ret
}

func GetTraceId() string {
	uniqId := uuid.NewV4().String()
	return uniqId
}

//iso-time 转换为 时间戳
func TransferISO2Timestamp(isoTime string) (int64, error) {
	theTime, err := time.ParseInLocation("2006-01-02 15:04:05", isoTime, time.Local)
	if err != nil {
		return 0, err
	}
	return theTime.Unix(), nil
}

func CalRequestSign(request map[string]string, secret string) string {
	//params
	params := make(map[string][]string)
	for k, v := range request {
		params[k] = []string{v}
	}
	//uv
	values := url.Values(params)
	encodes := values.Encode()
	encodes = strings.Replace(encodes, "+", "%20", -1)
	encodes = strings.Replace(encodes, "*", "%2A", -1)
	encodes = strings.Replace(encodes, "%7E", "~", -1)
	str := encodes + "&" + secret
	//sign
	return fmt.Sprintf("%x", md5.Sum([]byte(str)))
}

func CheckSum(data []byte) uint16 {
	var (
		sum    uint32
		length int = len(data)
		index  int
	)
	//以每16位为单位进行求和，直到所有的字节全部求完或者只剩下一个8位字节（如果剩余一个8位字节说明字节数为奇数个）
	for length > 1 {
		sum += uint32(data[index])<<8 + uint32(data[index+1])
		index += 2
		length -= 2
	}
	//如果字节数为奇数个，要加上最后剩下的那个8位字节
	if length > 0 {
		sum += uint32(data[index])
	}
	//加上高16位进位的部分
	sum += (sum >> 16)
	return uint16(^sum)
}

func OpenFile(path string, filename string, flag int, perm os.FileMode) (*os.File, error) {
	_, err := Mkdir(path, perm)
	if err != nil {
		return nil, fmt.Errorf("mkdir(%s) err:%s", path, err.Error())
	}
	file := fmt.Sprintf("%s/%s", path, filename)
	fd, err := os.OpenFile(file, flag, perm)
	if err != nil {
		return nil, fmt.Errorf("openFile(%s) err:%s", file, err.Error())
	}
	return fd, nil
}

func Mkdir(dir string, perm os.FileMode) (bool, error) {
	// 创建文件夹
	exist, err := PathExists(dir)
	if err != nil {
		return false, err
	}
	if exist {
		return false, nil
	} else {
		err := os.MkdirAll(dir, perm)
		if err != nil {
			return false, err
		} else {
			return true, nil
		}
	}
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}
