#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
}
