import random
import logging

import stablelog

# coordinator messages
from const2PC import VOTE_REQUEST, GLOBAL_COMMIT, GLOBAL_ABORT, PREPARE_COMMIT
# participant messages
from const2PC import VOTE_COMMIT, VOTE_ABORT, READY_COMMIT
# misc constants
from const2PC import TIMEOUT, TERM_TIMEOUT

class Coordinator:
    """
    Implements a two phase commit coordinator.
    - state written to stable log (but recovery is not considered)
    - simulates possible crash failure after vote request

    State machine 3PC:
      INIT -> WAIT -> PRECOMMIT -> COMMIT
                              -> ABORT (on any VOTE_ABORT or timeout)

    Simulated crash points:
      - before VOTE_REQUEST      (in INIT)
      - after  VOTE_REQUEST      (in WAIT)
      - after  PREPARE_COMMIT    (in PRECOMMIT)
    """

    def __init__(self, chan):
        self.channel = chan
        self.coordinator = self.channel.join('coordinator')
        self.participants = []  # list of all participants
        self.log = stablelog.create_log("coordinator-" + self.coordinator)
        self.stable_log = stablelog.create_log("coordinator-"
                                               + self.coordinator)
        self.logger = logging.getLogger("vs2lab.lab6.3pc.Coordinator")
        self.state = None

    def _enter_state(self, state):
        self.stable_log.info(state)  # Write to recoverable persistant log file
        self.logger.info("Coordinator {} entered state {}."
                         .format(self.coordinator, state))
        self.state = state

    def _crash(self, label):
        self.logger.warning("Coordinator {} CRASHED in {} (simulated)."
                            .format(self.coordinator, label))
        
    def init(self):
        self.channel.bind(self.coordinator)
        self._enter_state('INIT')  # Start in INIT state.

        # Prepare participant information.
        self.participants = self.channel.subgroup('participant')


# Helper
# ------
    def _collect_votes(self):
        """
        Phase 1: collect VOTE_COMMIT / VOTE_ABORT from all participants.
        Returns True iff all voted COMMIT.
        Enters ABORT and broadcasts GLOBAL_ABORT on the first VOTE_ABORT or timeout.
        """
        yet_to_receive = list(self.participants)
        while yet_to_receive:
            msg = self.channel.receive_from(self.participants, TIMEOUT)
            if not msg or msg[1] == VOTE_ABORT:
                reason = "timeout" if not msg else "VOTE_ABORT from " + msg[0]
                self._enter_state('ABORT')
                self.channel.send_to(self.participants, GLOBAL_ABORT)
                self.logger.info("Coordinator %s sent GLOBAL_ABORT. Reason: %s",
                                 self.coordinator, reason)
                return False
            assert msg[1] == VOTE_COMMIT
            yet_to_receive.remove(msg[0])
            self.logger.debug("Coordinator received VOTE_COMMIT from %s.", msg[0])
        return True

    def _collect_ready_commits(self):
        """
        Phase 3: collect READY_COMMIT from all participants.
        Returns True iff all acknowledged PRECOMMIT.
        If a participant times out in PRECOMMIT, we still commit (spec 3.2.2.a case 2).
        """
        yet_to_receive = list(self.participants)
        while yet_to_receive:
            msg = self.channel.receive_from(self.participants, TIMEOUT)
            if not msg:
                # Participant crashed in PRECOMMIT -> safe to commit (spec)
                self.logger.warning(
                    "Coordinator timeout waiting for READY_COMMIT - "
                    "assuming participant in PRECOMMIT, proceeding with COMMIT.")
                return True
            assert msg[1] == READY_COMMIT
            yet_to_receive.remove(msg[0])
            self.logger.debug("Coordinator received READY_COMMIT from %s.", msg[0])
        return True


# Main
# -------

    def run(self):
        # Crash point 1: before sending VOTE_REQUEST
        if random.random() > 3/4:  # simulate a crash
            self._crash('INIT')
            return "Coordinator crashed in state INIT."

        # Phase 1a: send VOTE_REQUEST
        self._enter_state('WAIT')
        self.channel.send_to(self.participants, VOTE_REQUEST)
        self.logger.info("Coordinator {} sent VOTE_REQUEST to {} participants."
                         .format(self.coordinator, len(self.participants)))

        # Crash point 2: after VOTE_REQUEST, before collecting votes
        if random.random() > 2/3:
            self._crash('WAIT')
            return "Coordinator crashed in WAIT (after VOTE_REQUEST)."

        # Phase 1b / 2a: collect votes
        if not self._collect_votes():
            return "Coordinator {} terminated in ABORT.".format(self.coordinator)

        # Phase 2a: all voted YES -> PRECOMMIT
        self._enter_state('PRECOMMIT')
        self.channel.send_to(self.participants, PREPARE_COMMIT)
        self.logger.info("Coordinator {} sent PREPARE_COMMIT.".format(self.coordinator))

        # Crash point 3: after PREPARE_COMMIT, before GLOBAL_COMMIT
        if random.random() > 2/3:
            self._crash('PRECOMMIT')
            return "Coordinator crashed in PRECOMMIT (after PREPARE_COMMIT)."

        # Phase 3a: collect READY_COMMIT acknowledgements
        self._collect_ready_commits()

        # Phase 3a: send GLOBAL_COMMIT
        self._enter_state('COMMIT')
        self.channel.send_to(self.participants, GLOBAL_COMMIT)
        self.logger.info("Coordinator {} sent GLOBAL_COMMIT.".format(self.coordinator))

        return "Coordinator {} terminated in COMMIT.".format(self.coordinator)
