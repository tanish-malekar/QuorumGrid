#!/usr/bin/env python3
"""Launch the five loopback JVMs; preserve data directories for restart exercises."""
import argparse
import pathlib
import signal
import subprocess
import time

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--data',type=pathlib.Path,default=pathlib.Path('cluster-data'))
    parser.add_argument('--port',type=int,default=52051)
    parser.add_argument('--jar',default='target/quorumgrid-1.0.0.jar')
    args=parser.parse_args()
    args.data.mkdir(parents=True,exist_ok=True)
    running=True
    def stop(*_):
        nonlocal running
        running=False
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
    processes=[];logs=[];reported=set()
    try:
        for node in range(5):
            directory=args.data/f'node-{node}';directory.mkdir(exist_ok=True)
            log=(directory/'process.log').open('ab');logs.append(log)
            command=['java','-cp',args.jar,'dev.tanish.quorum.Node',str(node),str(directory),str(args.port),str(directory/'faults.txt')]
            process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
            processes.append(process);(directory/'pid').write_text(str(process.pid)+'\n')
        print('Five nodes started; Ctrl-C stops the cluster.',flush=True)
        while running:
            for node,process in enumerate(processes):
                if process.poll() is not None and node not in reported:
                    print(f'Node {node} exited with {process.returncode}; see its process.log',flush=True);reported.add(node)
            time.sleep(.5)
    finally:
        for process in processes:
            if process.poll() is None:process.terminate()
        for process in processes:
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:process.kill();process.wait()
        for log in logs:log.close()
if __name__=='__main__':main()
