package logger

import "github.com/sirupsen/logrus"

// Interface logger interface
type Interface interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

var logger Interface

func init() {
	l := logrus.New()
	l.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	logger = l
}

func SetDefault(l Interface) {
	logger = l
}

func Info(args ...interface{}) {
	if logger == nil {
		return
	}

	logger.Info(args...)
}

func Infof(format string, args ...interface{}) {
	if logger == nil {
		return
	}

	logger.Infof(format, args...)
}

func Warn(args ...interface{}) {
	if logger == nil {
		return
	}

	logger.Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	if logger == nil {
		return
	}

	logger.Warnf(format, args...)
}

func Error(args ...interface{}) {
	if logger == nil {
		return
	}

	logger.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	if logger == nil {
		return
	}

	logger.Errorf(format, args...)
}

func LogIfError(err error) {
	if err != nil {
		Error(err)
	}
}
