'''Utilies for multiprocessing across ethernet with ZeroMQ'''

import sys
import time
import multiprocessing
import zmq

class Ventilator(multiprocessing.Process):
    '''ZeroMQ ventilator, produces tasks to run in parallel.

    Based on http://zguide.zeromq.org/py:taskvent.

    :param tasks: Iterable, each element is handed to a worker
    :param join_delay: Time to wait for workers to connect before starting
    '''
    def __init__(self, tasks, join_delay=6):
        self.tasks = tasks
        self.join_delay = join_delay
        multiprocessing.Process.__init__(self)

    def run(self):
        context = zmq.Context()
        sender = context.socket(zmq.PUSH)
        sender.bind("tcp://*:5557")

        sink = context.socket(zmq.PUSH)
        sink.connect("tcp://localhost:5558")

        print 'Ventilator: Waiting %1.0fs for workers to join...' % self.join_delay,
        sys.stdout.flush()
        time.sleep(self.join_delay)
        print 'done'

        for task in self.tasks:
            sender.send_pyobj(task)

        time.sleep(1)

        print "Ventilator: Done"


class Sink(multiprocessing.Process):
    '''ZeroMQ sink, collects results from workers.

    Based on http://zguide.zeromq.org/py:tasksink.

    :param task_count: The number of tasks the workers will process. After
                       task_count results are received, the workers are killed.
    :param output_address: Address for output Python objects (ZMQ PUSH socket)
    '''
    def __init__(self, task_count, output_address='ipc:///tmp/lxid0'):
        self.task_count = task_count
        self.output_address = output_address
        multiprocessing.Process.__init__(self)

    def run(self):
        context = zmq.Context()

        receiver = context.socket(zmq.PULL)
        receiver.bind("tcp://*:5558")

        controller = context.socket(zmq.PUB)
        controller.bind("tcp://*:5559")

        sink_data = context.socket(zmq.PUSH)
        sink_data.connect(self.output_address)

        tstart = time.time()

        for task_nbr in range(self.task_count):
            s = receiver.recv_pyobj()
            sink_data.send_pyobj(s)
            if task_nbr % 10 == 0:
                sys.stdout.write(':')
            else:
                sys.stdout.write('.')
            sys.stdout.flush()
        print

        controller.send('KILL')
        sink_data.send_pyobj(None)

        tend = time.time()
        print "Sink: Total elapsed time: %d msec" % ((tend-tstart)*1000)


if __name__ == '__main__':
    import glob
    from lxid.utils import convert_events_parallel
    from lxid.dataset import Cut
    files = glob.glob('/home/mastbaum/snoplus/tl208/data/pdf/tl208/run0/av_tl208-*0.root')
    cut = Cut(e=(2.555,2.718))
    r = convert_events_parallel(files, cut)
    print r

