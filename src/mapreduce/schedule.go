package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var task = make(chan int, 5)
	var finishTask = make(chan int)

	go func() {
		for i := 0; i < ntasks; i++ {
			debug("写入task %v \n", i)
			task <- i
		}
	}()

	for i := 0; i < ntasks; {
		select {
		case t := <-task:
			go func() {
				// 获取空闲worker
				workerSrv := <-registerChan
				args := DoTaskArgs{
					JobName:       jobName,
					Phase:         phase,
					TaskNumber:    t,
					NumOtherPhase: n_other,
				}
				if phase == mapPhase {
					args.File = mapFiles[t]
				}
				if call(workerSrv, "Worker.DoTask", args, nil) {
					// 通知完成
					finishTask <- t
				} else {
					task <- t
				}
				// 释放工人
				go func() { registerChan <- workerSrv }()
			}()

		case t := <-finishTask:
			debug("finish task:%v \n", t)
			i++
		default:
			debug("tasks %v/%v \n", i, ntasks)
		}
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
