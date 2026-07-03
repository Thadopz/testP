package engine

import "runtime"

func runtimeGosched() {
	runtime.Gosched()
}
