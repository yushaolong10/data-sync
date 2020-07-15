package logger

import zlog "github.com/shengkehua/xlog4go"

func Init(filePath string) error {
	if err := zlog.SetupLogWithConf(filePath); err != nil {
		return err
	}

	return nil
}

func Trace(fmt string, args ...interface{}) {
	zlog.Trace(fmt, args...)
}

func Debug(fmt string, args ...interface{}) {
	zlog.Debug(fmt, args...)
}

func Warn(fmt string, args ...interface{}) {
	zlog.Warn(fmt, args...)
}

func Info(fmt string, args ...interface{}) {

	zlog.Info(fmt, args...)

}

func Error(fmt string, args ...interface{}) {
	zlog.Error(fmt, args...)
}

func Public(fmt string, args ...interface{}) {
	zlog.Public(fmt, args...)
}

func Close() {
	zlog.Close()
}
