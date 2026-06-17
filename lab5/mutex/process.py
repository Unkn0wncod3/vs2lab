"""
Process with crash fault tolerance
"""

import logging
import random
import time

from constMutex import ENTER, RELEASE, ALLOW, ACTIVE, CRASHED, TIMEOUT_SECONDS, HEARTBEAT_SECONDS, HEARTBEAT

class Process:
    """
    Implements access management to a critical section (CS) via fully
    distributed mutual exclusion (MUTEX).

    Processes broadcast messages (ENTER, ALLOW, RELEASE) timestamped with
    logical (lamport) clocks. All messages are stored in local queues sorted by
    logical clock time.

    Processes follow different behavioral patterns. An ACTIVE process competes 
    with others for accessing the critical section. A PASSIVE process will never 
    request to enter the critical section itself but will allow others to do so.

    A process broadcasts an ENTER request if it wants to enter the CS. A process
    that doesn't want to ENTER replies with an ALLOW broadcast. A process that
    wants to ENTER and receives another ENTER request replies with an ALLOW
    broadcast (which is then later in time than its own ENTER request).

    A process enters the CS if a) its ENTER message is first in the queue (it is
    the oldest pending message) AND b) all other processes have sent messages
    that are younger (either ENTER or ALLOW). RELEASE requests purge
    corresponding ENTER requests from the top of the local queues.

    Message Format:

    <Message>: (Timestamp, Process_ID, <Request_Type>)

    <Request Type>: ENTER | ALLOW  | RELEASE

    """

    def __init__(self, chan):
        self.channel = chan  # Create ref to actual channel
        self.process_id = self.channel.join('proc')  # Find out who you are
        self.all_processes: list = []  # All procs in the proc group
        self.other_processes: list = []  # Needed to multicast to others
        self.queue = []  # The request queue list
        self.clock = 0  # The current logical clock
        self.peer_name = 'unassigned'  # The original peer name
        self.peer_type = 'unassigned'  # A flag indicating behavior pattern
        self.crashed_processes: set = set()  # Set of known crashed process IDs
        self.last_heard: dict = {}   # pid → float (time.time())
        self.logger = logging.getLogger("vs2lab.lab5.mutex.process.Process")

    def __mapid(self, id='-1'):
        # format channel member address
        if id == '-1':
            id = self.process_id
        return 'Proc-'+str(id)

    def __cleanup_queue(self):
        if len(self.queue) > 0:
            # self.queue.sort(key = lambda tup: tup[0])
            self.queue.sort()
            # There should never be old ALLOW messages at the head of the queue
            while self.queue[0][2] == ALLOW:
                del (self.queue[0])
                if len(self.queue) == 0:
                    break

    def __request_to_enter(self):
        self.clock = self.clock + 1  # Increment clock value
        request_msg = (self.clock, self.process_id, ENTER)
        self.queue.append(request_msg)  # Append request to queue
        self.__cleanup_queue()  # Sort the queue
        self.channel.send_to(self.other_processes, request_msg)  # Send request

    def __allow_to_enter(self, requester):
        self.clock = self.clock + 1  # Increment clock value
        msg = (self.clock, self.process_id, ALLOW)
        self.channel.send_to([requester], msg)  # Permit other

    def __release(self):
        # need to be first in queue to issue a release
        assert self.queue[0][1] == self.process_id, 'State error: inconsistent local RELEASE'

        # construct new queue from later ENTER requests (removing all ALLOWS)
        tmp = [r for r in self.queue[1:] if r[2] == ENTER]
        self.queue = tmp  # and copy to new queue
        self.clock = self.clock + 1  # Increment clock value
        msg = (self.clock, self.process_id, RELEASE)
        # Multicast release notification
        self.channel.send_to(self.other_processes, msg)

    def __allowed_to_enter(self):
        # See who has sent a message (the set will hold at most one element per sender)
        processes_with_later_message = set([req[1] for req in self.queue[1:]])
        # Access granted if this process is first in queue and all others have answered (logically) later
        first_in_queue = self.queue[0][1] == self.process_id
        all_have_answered = len(self.other_processes) == len(
            processes_with_later_message)
        return first_in_queue and all_have_answered

    def __handle_crash(self, crashed_pid):
        """Remove a crashed process from the coordination group."""
        if crashed_pid not in self.other_processes:
            return
        
        self.logger.warning("{} detected CRASH of {} - removing from group.".format(
            self.__mapid(), self.__mapid(crashed_pid)))
        
        # Remove from process lists
        self.other_processes.remove(crashed_pid)
        if crashed_pid in self.all_processes:
            self.all_processes.remove(crashed_pid)
        self.crashed_processes.add(crashed_pid)

        # Broadcast CRASHED so other peers skip their own timeout wait
        if self.other_processes:
            self.clock += 1
            self.channel.send_to(self.other_processes, (self.clock, crashed_pid, CRASHED))


        # Remove this process's messages from the queue
        before = len(self.queue)
        self.queue = [msg for msg in self.queue if msg[1] != crashed_pid]
        if len(self.queue) != before:
            self.logger.info("{} purged {} queue entries from crashed process {}.".format(
                self.__mapid(), before - len(self.queue), self.__mapid(crashed_pid)))

        if self.queue:
            self.__cleanup_queue()

    def __detect_crashes(self):
        """        
        Called after every receive timeout.
        Any peer silent for >= TIMEOUT_SECONDS is considered crashed.
        Wall-clock time is used so old queue entries don't mask silence.
        """
        time_now = time.time()
        for pid in list(self.other_processes):
            last_seen = self.last_heard.get(pid, self.init_time)
            if time_now - last_seen >= TIMEOUT_SECONDS:
                self.__handle_crash(pid)

    def __send_heartbeat(self):
        if time.time() - self.last_heard[self.process_id] >= HEARTBEAT_SECONDS:
            self.clock += 1
            msg = (self.clock, self.process_id, HEARTBEAT)
            self.channel.send_to(self.other_processes, msg)
            self.last_heard[self.process_id] = time.time()

    def __receive(self):
        if not self.other_processes:   # Return if crashed
            return
        _receive = self.channel.receive_from(self.other_processes, 3)
        if _receive:
            msg = _receive[1]
            sender = _receive[0]
            self.last_heard[sender] = time.time()  # Tracks last time alive

            self.clock = max(self.clock, msg[0])
            self.clock = self.clock + 1

            self.logger.debug("{} received {} from {}.".format(
                self.__mapid(),
                "ENTER" if msg[2] == ENTER
                else "ALLOW" if msg[2] == ALLOW
                else "RELEASE", self.__mapid(msg[1])))

            if msg[2] == ENTER:
                # Ignore messages from known crashed processes
                if msg[1] in self.crashed_processes:
                    return
                self.queue.append(msg)
                self.__allow_to_enter(msg[1])
            elif msg[2] == ALLOW:
                if msg[1] in self.crashed_processes:
                    return
                self.queue.append(msg)
            elif msg[2] == HEARTBEAT:
                return
            elif msg[2] == RELEASE:
                # Ignore RELEASE from crashed processes
                if msg[1] in self.crashed_processes:
                    return
                # Guard: only remove if it's actually at the head of the queue
                if self.queue and self.queue[0][1] == msg[1] and self.queue[0][2] == ENTER:
                    del (self.queue[0])
                else:
                    self.logger.warning("{} received unexpected RELEASE from {} - ignoring.".format(
                        self.__mapid(), self.__mapid(msg[1])))
                    return
            elif msg[2] == CRASHED:
                crashed_process = msg[1]
                if not crashed_process in self.crashed_processes:
                    self.__handle_crash(msg[1])

            self.__cleanup_queue()
        else:
            # Timeout: check for crashes
            self.logger.info("{} timed out on RECEIVE. Checking for crashes. Local queue: {}".format(
                self.__mapid(),
                list(map(lambda msg: (
                    'Clock ' + str(msg[0]),
                    self.__mapid(msg[1]),
                    msg[2]), self.queue))))
            self.__detect_crashes()

    def init(self, peer_name, peer_type):
        self.last_heard[self.process_id] = time.time()
        self.init_time: float = time.time()

        self.channel.bind(self.process_id)

        self.all_processes = list(self.channel.subgroup('proc'))
        # sort string elements by numerical order
        self.all_processes.sort(key=lambda x: int(x))

        self.other_processes = list(self.channel.subgroup('proc'))
        self.other_processes.remove(self.process_id)

        self.peer_name = peer_name  # assign peer name
        self.peer_type = peer_type  # assign peer behavior

        self.logger.info("{} joined channel as {}.".format(
            peer_name, self.__mapid()))

    def run(self):
        while True:
            # Enter the critical section if
            # 1) there are more than one process left and
            # 2) this peer has active behavior and
            # 3) random is true
            self.__send_heartbeat() #! heartbeat
            if len(self.all_processes) > 1 and \
                    self.peer_type == ACTIVE and \
                    random.choice([True, False]):
                self.logger.debug("{} wants to ENTER CS at CLOCK {}."
                                  .format(self.__mapid(), self.clock))

                self.__request_to_enter()
                while not self.__allowed_to_enter():
                    self.__send_heartbeat() #! heartbeat while wait
                    self.__receive()

                # Stay in CS for some time ...
                sleep_time = random.randint(0, 2000)
                self.logger.debug("{} enters CS for {} milliseconds."
                                  .format(self.__mapid(), sleep_time))
                print(" CS <- {}".format(self.__mapid()))
                time.sleep(sleep_time/1000)

                # ... then leave CS
                print(" CS -> {}".format(self.__mapid()))
                self.__release()
                continue

            # Occasionally serve requests to enter (
            if random.choice([True, False]):
                self.__receive()
