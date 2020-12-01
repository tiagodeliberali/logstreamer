enum ServerRole {
    Leader,
    Follower,
    Candidate,
}

enum LogAction {

}

pub struct LogEntry {
    index: u32,
    term: u32,
    action: LogAction,
}

pub struct Server {
    // persisted state
    current_term: u32,
    voted_for: Option<u32>,
    log: Vec<LogEntry>,

    // volatile state
    commit_index: u32,
    last_applied: u32,

    // extra
    role: ServerRole,
}

pub struct Leader {
    next_index: Vec<(u32, u32)>,
    match_index: Vec<(u32, u32)>,
}

pub struct Follower {

}

impl Follower {
    pub fn append_entry(args: AppendEntryArgs) -> ResultArgs {
        ResultArgs::new(0, false)
    }

    pub fn request_vote(args: RequestVoteArgs) -> ResultArgs {
        ResultArgs::new(0, false)
    }
}

pub struct Candidate {

}

impl Candidate {
    pub fn append_entry(args: AppendEntryArgs) -> ResultArgs {
        ResultArgs::new(0, false)
    }
}

pub struct AppendEntryArgs {
    term: u32,
    leader_id: u32, 
    previous_log_index: u32, 
    previous_log_term: u32, 
    entries: Vec<LogEntry>, 
    leader_commit: u32
}

pub struct RequestVoteArgs {
    term: u32, 
    candidate_id: u32, 
    last_log_index: u32, 
    last_log_term: u32,
}

pub struct ResultArgs {
    term: u32, 
    result: bool,
}

impl ResultArgs {
    pub fn new(term: u32, result: bool,) -> ResultArgs {
        ResultArgs {
            term,
            result
        }
    }
}

enum RPC {
    AppendEntry(AppendEntryArgs),
    RequestVote(RequestVoteArgs),
    Result(ResultArgs),
}