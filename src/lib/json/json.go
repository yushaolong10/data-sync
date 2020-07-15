package json

import (
	jsLibrary "github.com/json-iterator/go"
	"io"
)

var defaultJsonLib = jsLibrary.ConfigCompatibleWithStandardLibrary

func Marshal(v interface{}) ([]byte, error) {
	return defaultJsonLib.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return defaultJsonLib.Unmarshal(data, v)
}

func UnmarshalFromString(str string, v interface{}) error {
	return defaultJsonLib.UnmarshalFromString(str, v)
}

func NewEncoder(writer io.Writer) *jsLibrary.Encoder {
	return defaultJsonLib.NewEncoder(writer)
}

func NewDecoder(reader io.Reader) *jsLibrary.Decoder {
	return defaultJsonLib.NewDecoder(reader)
}
