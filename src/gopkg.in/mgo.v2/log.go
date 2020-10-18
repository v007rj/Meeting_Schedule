
package mgo

import (
	"fmt"
	"sync"
)


type log_Logger interface {
	Output(calldepth int, s string) error
}

var (
	globalLogger log_Logger
	globalDebug  bool
	globalMutex  sync.Mutex
)


func SetLogger(logger log_Logger) {
	if raceDetector {
		globalMutex.Lock()
		defer globalMutex.Unlock()
	}
	globalLogger = logger
}


func SetDebug(debug bool) {
	if raceDetector {
		globalMutex.Lock()
		defer globalMutex.Unlock()
	}
	globalDebug = debug
}

func log(v ...interface{}) {
	if raceDetector {
		globalMutex.Lock()
		defer globalMutex.Unlock()
	}
	if globalLogger != nil {
		globalLogger.Output(2, fmt.Sprint(v...))
	}
}

func logln(v ...interface{}) {
	if raceDetector {
		globalMutex.Lock()
		defer globalMutex.Unlock()
	}
	if globalLogger != nil {
		globalLogger.Output(2, fmt.Sprintln(v...))
	}
}

func logf(format string, v ...interface{}) {
	if raceDetector {
		globalMutex.Lock()
		defer globalMutex.Unlock()
	}
	if globalLogger != nil {
		globalLogger.Output(2, fmt.Sprintf(format, v...))
	}
}

func debug(v ...interface{}) {
	if raceDetector {
		globalMutex.Lock()
		defer globalMutex.Unlock()
	}
	if globalDebug && globalLogger != nil {
		globalLogger.Output(2, fmt.Sprint(v...))
	}
}

func debugln(v ...interface{}) {
	if raceDetector {
		globalMutex.Lock()
		defer globalMutex.Unlock()
	}
	if globalDebug && globalLogger != nil {
		globalLogger.Output(2, fmt.Sprintln(v...))
	}
}

func debugf(format string, v ...interface{}) {
	if raceDetector {
		globalMutex.Lock()
		defer globalMutex.Unlock()
	}
	if globalDebug && globalLogger != nil {
		globalLogger.Output(2, fmt.Sprintf(format, v...))
	}
}
