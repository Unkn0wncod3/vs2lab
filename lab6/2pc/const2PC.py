
# coordinator messages 
## Phase 1
VOTE_REQUEST = 'VOTE_REQUEST'
## Phase 2
PREPARE_COMMIT  = 'PREPARE_COMMIT'   # all voted YES
GLOBAL_ABORT = 'GLOBAL_ABORT'
## Phase 3
GLOBAL_COMMIT   = 'GLOBAL_COMMIT'
## coordinator crash
STATE_QUERY     = 'STATE_QUERY'      # new coordinator asks all: "what state?"
STATE_REPORT    = 'STATE_REPORT'     # participant answers with its state

# participant messages
## Phase 1
VOTE_COMMIT = 'VOTE_COMMIT'
VOTE_ABORT = 'VOTE_ABORT'
## Phase 2
READY_COMMIT    = 'READY_COMMIT'     # participant ACK PRECOMMIT
# ?
NEED_DECISION = 'NEED_DECISION'

# participant decisions
LOCAL_ABORT = 'LOCAL_ABORT'
LOCAL_SUCCESS = 'LOCAL_SUCCESS'
TERM_ALIVE = 'TERM_ALIVE'

# Timeouts (seconds)
TIMEOUT      = 1 # fail-noisy crash timeout
TERM_TIMEOUT = 3 # state-query