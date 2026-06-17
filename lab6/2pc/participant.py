import random
import logging

# coordinator messages
from const2PC import VOTE_REQUEST, PREPARE_COMMIT, GLOBAL_COMMIT, GLOBAL_ABORT
# participant decissions
from const2PC import LOCAL_SUCCESS, LOCAL_ABORT, READY_COMMIT
# participant messages
from const2PC import VOTE_COMMIT, VOTE_ABORT
# misc constants
from const2PC import TIMEOUT, TERM_TIMEOUT, STATE_QUERY, STATE_REPORT, TERM_ALIVE

import stablelog


class Participant:
    """
    Implements a two phase commit participant.
    - state written to stable log (but recovery is not considered)
    - in case of coordinator crash, participants mutually synchronize states
    - system blocks if all participants vote commit and coordinator crashes
    - allows for partially synchronous behavior with fail-noisy crashes


    State machine 3PC:
      INIT -> READY -> PRECOMMIT -> COMMIT
           -> ABORT

    Termination protocol (coordinator crashed):
      The participant with the smallest ID becomes the new coordinator.
      It broadcasts STATE_QUERY, collects STATE_REPORT from all peers, then drives
      the outcome according to the 3PC termination rules:

      - new-coord in WAIT      -> GLOBAL_ABORT  (someone may be in INIT/ABORT)
      - new-coord in PRECOMMIT -> GLOBAL_COMMIT (all others are in READY/PRECOMMIT/COMMIT)
      - new-coord in COMMIT    -> GLOBAL_COMMIT (already decided)
      - new-coord in ABORT     -> GLOBAL_ABORT  (already decided)
    """

    def __init__(self, chan):
        self.channel = chan
        self.participant = self.channel.join('participant')
        self.stable_log = stablelog.create_log(
            "participant-" + self.participant)
        self.logger = logging.getLogger("vs2lab.lab6.3pc.Participant")
        self.coordinator = {}
        self.all_participants = {}
        self.state = 'NEW'

    @staticmethod
    def _do_work():
        # Simulate local activities that may succeed or not
        return LOCAL_ABORT if random.random() > 2/3 else LOCAL_SUCCESS

    def _enter_state(self, state):
        self.stable_log.info(state)  # Write to recoverable persistant log file
        self.logger.info("Participant {} entered state {}."
                         .format(self.participant, state))
        self.state = state

    def init(self):
        self.channel.bind(self.participant)
        self.coordinator = self.channel.subgroup('coordinator')
        self.all_participants = self.channel.subgroup('participant')
        self.other_participants = [
            participants for participants in self.all_participants
                if participants != self.participant
            ]
        self._enter_state('INIT')  # Start in local INIT state.


# coordinator crashed
# ---------

    def _elect_new_coordinator(self):
        #"""Deterministic election: smallest participant ID becomes new coordinator."""
        #sorted_pids = sorted(self.all_participants, key=lambda x: int(x))
        #return sorted_pids[0]

        self.channel.send_to(self.other_participants, (TERM_ALIVE, self.state))
        alive = {self.participant: self.state}

        for _ in range(len(self.other_participants)):
            msg = self.channel.receive_from(self.all_participants, TERM_TIMEOUT)
            if msg and isinstance(msg[1], tuple) and msg[1][0] == TERM_ALIVE:
                alive[msg[0]] = msg[1][1]

        
        return min(alive.keys(), key=lambda x: int(x))

    def _termination_protocol(self):
        """
        Non-blocking termination after coordinator crash.

        1. Elect new coordinator (smallest PID).
        2. New coordinator broadcasts STATE_QUERY and collects STATE_REPORT.
        3. Decision is driven by the collected states (3PC rules).
        4. All participants apply the final decision.
        """
        new_coord = self._elect_new_coordinator()

        self.logger.info(
            "Participant {} starting termination. New coordinator: {} (am I? {})"
                .format(self.participant, new_coord, new_coord == self.participant))

        if new_coord == self.participant:
            return self._termination_as_coordinator()
        else:
            return self._termination_as_follower(new_coord)

    def _termination_as_coordinator(self):
        """
        Drive termination as the newly elected coordinator.
        Collect STATE_REPORT from all others, decide, broadcast.
        """
        self.logger.info("Participant {} acting as NEW COORDINATOR.".format(self.participant))

        # Broadcast state query to all peers
        self.channel.send_to(
            {p for p in self.all_participants if p != self.participant},
            (STATE_QUERY, self.state))

        # Collect state reports
        known_states = {self.participant: self.state}
        for _ in range(len(self.other_participants)):
            msg = self.channel.receive_from(self.all_participants, TERM_TIMEOUT *2)
            if msg:
                pid, payload = msg[0], msg[1]
                if isinstance(payload, tuple) and payload[0] == STATE_REPORT:
                    known_states[pid] = payload[1]
                    self.logger.debug("New-coord got STATE_REPORT {} from {} "
                                      .format(payload[1], pid))
            # Missing participant -> treat as INIT (most conservative)

        self.logger.info("New-coord state overview: {}".format(known_states))

        # Termination decision rules
        if self.state == 'READY':
            self._enter_state('WAIT') # elected coordinator state
            # change state to ABORT 
            self._enter_state('ABORT')
            decision = GLOBAL_ABORT
        elif self.state == 'PRECOMMIT':
            self._enter_state('COMMIT')
            decision = GLOBAL_COMMIT
        elif self.state == 'COMMIT':
            decision = GLOBAL_COMMIT
        else: # self.state == 'ABORT'
            decision = GLOBAL_ABORT

        # Broadcast decision
        all_peers = {participant for participant in self.all_participants if participant != self.participant}
        self.logger.info("New-coord {} decided {} and broadcast to peers."
                         .format(self.participant, decision))
        self.channel.send_to(all_peers, decision)
        return decision

    def _termination_as_follower(self, new_coord):
        """
        Participate in termination as a follower.
        Reply to STATE_QUERY, then wait for the final decision.
        """
        # Wait for STATE_QUERY from new coordinator
        msg = self.channel.receive_from(self.all_participants, TERM_TIMEOUT)
        if msg and isinstance(msg[1], tuple) and msg[1][0] == STATE_QUERY:
            self.channel.send_to({msg[0]}, (STATE_REPORT, self.state))
            self.logger.debug("Participant {} reported state {} to new-coord."
                              .format(self.participant, self.state))

        # Wait for decision from new coordinator
        msg = self.channel.receive_from(self.all_participants, TERM_TIMEOUT * (len(self.all_participants) + 3))
        if msg and msg[1] in (GLOBAL_COMMIT, GLOBAL_ABORT):
            return msg[1]

        # Fallback: if new-coord also crashed -> conservative abort
        self.logger.warning(
            "Participant {} got no decision from new-coord - defaulting to ABORT.".format(self.participant))
        return GLOBAL_ABORT
    
    # Main
    # -----------

    def run(self):
        # Phase 1b: wait for VOTE_REQUEST
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:
            # Coordinator crashed before VOTE_REQUEST -> safe to abort (INIT state)
            self._enter_state('ABORT')
            self.logger.warning(
                "Participant {}: no VOTE_REQUEST received - coordinator crashed in INIT."
                        .format(self.participant))
            return "Participant {} terminated in ABORT (coordinator crashed in INIT)."                .format(self.participant)

        assert msg[1] == VOTE_REQUEST

        # Perform local work
        decision = self._do_work()

        if decision == LOCAL_ABORT:
            self._enter_state('ABORT')
            self.channel.send_to(self.coordinator, VOTE_ABORT)
            self.logger.info("Participant {} sent VOTE_ABORT.".format(self.participant))
            return "Participant {} terminated in ABORT (local failure).".format(self.participant)

        # Local success -> enter READY, vote COMMIT
        self._enter_state('READY')
        self.channel.send_to(self.coordinator, VOTE_COMMIT)
        self.logger.info("Participant {} sent VOTE_COMMIT.".format(self.participant))

        # Phase 2b: wait for PREPARE_COMMIT or GLOBAL_ABORT
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:
            # Coordinator crashed in WAIT or PRECOMMIT -> termination protocol
            self.logger.warning(
                "Participant {}: coordinator crashed after VOTE_REQUEST -> termination.".format(self.participant))
            decision_msg = self._termination_protocol()
            final = 'COMMIT' if decision_msg == GLOBAL_COMMIT else 'ABORT'
            self._enter_state(final)
            return "Participant {} terminated in {} via termination protocol.".format(self.participant, final)

        if msg[1] == GLOBAL_ABORT:
            self._enter_state('ABORT')
            return "Participant {} terminated in ABORT (GLOBAL_ABORT).".format(self.participant)

        assert msg[1] == PREPARE_COMMIT

        # Phase 2b: enter PRECOMMIT, acknowledge
        self._enter_state('PRECOMMIT')
        self.channel.send_to(self.coordinator, READY_COMMIT)
        self.logger.info("Participant {} sent READY_COMMIT.".format(self.participant))

        # Phase 3b: wait for GLOBAL_COMMIT
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:
            # Coordinator crashed in PRECOMMIT -> termination protocol
            self.logger.warning(
                "Participant {}: coordinator crashed in PRECOMMIT -> termination.".format(self.participant))
            decision_msg = self._termination_protocol()
            # In PRECOMMIT, termination always -> COMMIT (spec guarantees this)
            final = 'COMMIT' if decision_msg == GLOBAL_COMMIT else 'ABORT'
            self._enter_state(final)
            return "Participant {} terminated in {} via termination protocol.".format(self.participant, final)

        assert msg[1] == GLOBAL_COMMIT
        self._enter_state('COMMIT')
        return "Participant {} terminated in COMMIT.".format(self.participant)
