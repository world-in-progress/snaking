package main

import (
	"fmt"
	"os"
	"os/exec"
	"snaking/orchestrator"
)

type FdbOp string

const (
	Create  FdbOp = "--create"
	Read    FdbOp = "--read"
	ShmRead FdbOp = "--shm-read"
	ShmWait FdbOp = "--shm-wait"
	Clean   FdbOp = "--clean"

	FdbPath = "test.fdb"
	FdbShm  = "shm-fdb"
)

func fdbExec(op FdbOp, extraFile *os.File, args ...string) {
	cmdArgs := []string{
		"run",
		"./pytests/main.py",
		string(op),
		"--path", args[0],
	}
	if op == ShmWait {
		cmdArgs = append(cmdArgs, "--fd", args[1])
	}

	cmd := exec.Command(
		"uv",
		cmdArgs...,
	)
	if extraFile != nil {
		cmd.ExtraFiles = []*os.File{extraFile}
	}

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fmt.Printf("Error running command: %v\n", err)
		return
	}
}

func main() {
	// pipe, err := semaphorepipe.New()
	// if err != nil {
	// 	fmt.Printf("Error creating semaphore pipe: %v\n", err)
	// 	return
	// }
	// defer pipe.Close()

	// fdb, err := asset.NewMirrorAsset(FdbPath, false)
	// fdbExec(Create, nil, fdb.Where())
	// if err != nil {
	// 	fmt.Printf("Error creating asset: %v\n", err)
	// 	return
	// }
	// defer fdbExec(Clean, nil, fdb.Where())

	// if err := fdb.ShareTo(FdbShm); err != nil {
	// 	fmt.Printf("Error sharing asset: %v\n", err)
	// 	return
	// }

	// // go fdbExec(ShmWait, pipe.Receiver, FdbShm, fmt.Sprintf("%d", pipe.Receiver.Fd()))
	// fmt.Printf("fd: %d\n", int(pipe.Receiver.Fd()))
	// time.Sleep(10 * time.Second)
	// pipe.Post()

	metaInfo := &orchestrator.MetaInfo{
		AssetPath: "./",
		WorkerList: []string{
			"proprocessor-001",
		},
	}

	server, _ := orchestrator.New(metaInfo)
	go server.Run("/tmp/controller.sock")

	select {}
}
