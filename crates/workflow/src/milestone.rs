// M7.3: Milestone state machine
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum MilestoneStatus { Draft, InProgress, Reviewing, Completed, Graduated }
pub struct Milestone { pub name: String, pub status: MilestoneStatus }
impl Milestone { pub fn new(name: &str) -> Self { Self{name:name.to_string(),status:MilestoneStatus::Draft} } pub fn advance(&mut self) { self.status = match self.status { MilestoneStatus::Draft=>MilestoneStatus::InProgress, MilestoneStatus::InProgress=>MilestoneStatus::Reviewing, MilestoneStatus::Reviewing=>MilestoneStatus::Completed, _=>self.status.clone() }; } }
