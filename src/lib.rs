pub mod client;

use std::collections::HashMap;
use std::sync::Mutex;
use tonic::{Code, Request, Response, Status};
pub use crate::coordinator_server::{Coordinator, CoordinatorServer};

tonic::include_proto!("work");

#[derive(Debug, PartialEq)]
enum ReduceStatus {
    Ready,
    Scheduled,
    Done,
}

#[derive(Debug, PartialEq)]
enum MapStatus {
    Ready,
    Scheduled,
    Done,
}

#[derive(Debug, PartialEq)]
struct Partition {
    id: u32,
    files: Vec<String>,
    status: ReduceStatus,
}

pub struct State {
    files: HashMap<String, MapStatus>,
    partitions: Vec<Partition>,
    output: Vec<String>,
}

impl State {
    pub fn new(num_partitions: usize, files: Vec<String>) -> Self {
        let mut partitions = Vec::with_capacity(num_partitions);
        for i in 0..num_partitions {
            partitions.push(Partition{id: i as u32, files: vec![], status: ReduceStatus::Ready});
        }
        let mut filemap = HashMap::new();
        for file in files.iter() {
            filemap.insert(file.to_owned(), MapStatus::Ready);
        }
        State{files: filemap, partitions, output: Vec::new()}
    }

    pub fn get_file_to_map(&mut self) -> Option<String> {
        for (key, val) in self.files.iter_mut() {
            if *val == MapStatus::Ready {
                *val = MapStatus::Scheduled;
                return Some(key.clone());
            }
        }
        None
    }

    pub fn mark_map_file_done(&mut self, file: String) {
        *self.files.get_mut(&file).unwrap() = MapStatus::Done;
    }

    pub fn is_done_mapping(&self) -> bool {
        for v in self.files.values() {
            if *v != MapStatus::Done {
                return false;
            }
        }
        true
    }

    pub fn add_mapped_files(&mut self, files: Vec<(u8, String)>) -> Result<(), Box<dyn std::error::Error>> {
        for pair in files.iter().take_while(|_| true) {
            // dbg!(&self.partitions);
            match self.partitions.get_mut(pair.0 as usize) {
                Some(mut partition) => {
                    partition.files.push(pair.1.clone());
                },
                None => {
                    println!("No partition for {}, unable to add file {}", pair.0, pair.1);
                }
            }
        }

        Ok(())
    }

    pub fn complete_partition(&mut self, partition: usize, output: String) -> Result<(), Box<dyn std::error::Error>> {
        println!("Completing partition {}. output {}", partition, &output);
        self.partitions[partition].status = ReduceStatus::Done;
        self.output.push(output);
        Ok(())
    }
}

#[cfg(test)]
pub mod state_tests {
    use crate::*;

    #[test]
    pub fn test_get_file_to_map() {
        let mut filemap = HashMap::new();
        filemap.insert(String::from("file_0"), MapStatus::Ready);
        let mut state = State{
            files: filemap,
            partitions: vec![], // this test does not require valid state for parition or output
            output: vec![],
        };

        assert_eq!(Some(String::from("file_0")), state.get_file_to_map());
    }

    #[test]
    pub fn test_done_mapping() {
        let mut filemap = HashMap::new();
        let filekey = String::from("file_0");
        filemap.insert(filekey, MapStatus::Ready);
        let mut state = State{
            files: filemap,
            partitions: vec![], // this test does not require valid state for parition or output
            output: vec![],
        };

        assert!(!state.is_done_mapping()); // not done mapping

        *state.files.get_mut("file_0").unwrap() = MapStatus::Scheduled;
        assert!(!state.is_done_mapping()); // not done mapping

        *state.files.get_mut("file_0").unwrap() = MapStatus::Done;
        dbg!(&state.files);
        assert!(state.is_done_mapping()); // done mapping
    }

    #[test]
    pub fn test_add_mapped_files() {
        let mut state = State::new(3, vec![]);
        let input_files = vec![(0, "file_0".to_owned()), (1, "file_1".to_owned()), (2, "file_2".to_owned())];
        state.add_mapped_files(input_files);

        let expected = vec![
            Partition{id: 0,
                      files: vec!["file_0".to_owned()],
                      status: ReduceStatus::Ready
            },
            Partition{id: 1,
                      files: vec!["file_1".to_owned()],
                      status: ReduceStatus::Ready
            },
            Partition{id: 2,
                      files: vec!["file_2".to_owned()],
                      status: ReduceStatus::Ready
            }
        ];

        assert_eq!(expected, state.partitions);
    }
}

pub struct Coordinate {
    pub state_guard: Mutex<State>,
}

impl Coordinate {
    pub fn new(state: State) -> Self {
        Coordinate{state_guard: Mutex::new(state)}
    }
}

#[tonic::async_trait]
impl Coordinator for Coordinate {
    async fn get_work(&self, request: Request<GetWorkRequest>) -> Result<Response<GetWorkReply>, Status> {
        let mut state_guard = &mut self.state_guard.lock().unwrap();
        if state_guard.is_done_mapping() {
            for part in state_guard.partitions.iter_mut() {
                if part.status == ReduceStatus::Ready {
                    let reply = GetWorkReply{work_type: String::from("reduce"), files: part.files.clone(), partition: part.id};
                    part.status = ReduceStatus::Scheduled;
                    return Ok(Response::new(reply));
                }
            }
            let reply = GetWorkReply{work_type: String::from("done"), files: vec![], partition: u32::MAX};
            return Ok(Response::new(reply));
        } else {
            match state_guard.get_file_to_map() {
                Some(file) => {
                    let files = vec![file];
                    let reply = GetWorkReply{work_type: String::from("map"), files, partition: u32::MAX};
                    return Ok(Response::new(reply));
                },
                None => {
                    println!("None received when retrieving next file to map");
                    return Err(Status::not_found("Unable to retrieve a file to be mapped"))
                }
            };
        }
    }

    async fn complete_work(&self, request: Request<CompleteWorkRequest>) -> Result<Response<CompleteWorkReply>, Status> {
        println!("Got complete work request");
        let mut state = &mut self.state_guard.lock().unwrap();
        let work_type = &request.get_ref().work_type;
        match work_type.as_str() {
            "map" => {
                let files: Vec<(u8, String)> = request.get_ref().files.iter()
                    .map(|(k, v)| (k.parse::<u8>().unwrap(), v.to_owned()))
                    .collect();
                state.add_mapped_files(files);
                state.mark_map_file_done(request.get_ref().original_file.to_owned());
                return Ok(Response::new(CompleteWorkReply{}));
            },
            "reduce" => {
                let (partition, reduce_output_file) = request.get_ref().files.iter().nth(0).unwrap();
                match partition.parse::<usize>() {
                    Ok(val) => {
                        state.complete_partition(val, reduce_output_file.to_string());
                        return Ok(Response::new(CompleteWorkReply{}));
                    },
                    Err(e) => {
                        println!("ERROR parsing partition {}. {:?}", partition, e);
                        return Err(Status::new(Code::Internal, "failed"));
                    }
                }
            },
            _ => panic!("Unknown work_type {}", work_type),
        }
    }
}

#[cfg(test)]
mod coordinator_tests {
    use crate::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn test_get_work_map() -> TestResult {
        let files = vec![String::from("file-one")];
        let state = State::new(1, files);
        let coordinate = Coordinate::new(state);

        let work_request = tonic::Request::new(GetWorkRequest {
            id: String::from("test"),
        });

        let work_response = coordinate.get_work(work_request).await?;

        assert_eq!(1, work_response.get_ref().files.len());
        dbg!(&work_response);
        assert_eq!(String::from("map"), work_response.get_ref().work_type);
        assert_eq!(String::from("file-one"), work_response.get_ref().files[0]);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_work_wait_on_map_to_finish() -> TestResult {
        let mut files = HashMap::new();
        files.insert(String::from("file-one"), MapStatus::Scheduled);
        let state = State{
            files: files,
            partitions: vec![],
            output: vec![],
        };
        let coordinate = Coordinate::new(state);

        let work_request = tonic::Request::new(GetWorkRequest {
            id: String::from("test"),
        });

        let work_response = coordinate.get_work(work_request).await.expect_err("expected error Status::Code::NotFound");
        assert_eq!(tonic::Code::NotFound, work_response.code());
        dbg!(work_response);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_work_reduce() -> TestResult {
        let files = vec![]; // all input files are done
        let mut state = State::new(1, files);
        state.add_mapped_files(vec![(0, "file_1_part_0".to_owned()),
                                    (0, "file_2_part_0".to_owned())]);

        let coordinate = Coordinate::new(state);

        let work_request = tonic::Request::new(GetWorkRequest {
            id: String::from("test"),
        });

        let work_response = coordinate.get_work(work_request).await?;
        assert_eq!(2, work_response.get_ref().files.len());
        assert_eq!(String::from("reduce"), work_response.get_ref().work_type);
        assert_eq!(vec![String::from("file_1_part_0"), String::from("file_2_part_0")], work_response.get_ref().files);

        Ok(())
    }

    #[tokio::test]
    async fn test_complete_work_map() -> TestResult {
        let files = vec![String::from("file-one"), String::from("file_two")];
        let state = State::new(2, files);
        let coordinate = Coordinate::new(state);

        let mut mapped_file_one = HashMap::new();
        mapped_file_one.insert("0".to_owned(), "file-one_0".to_owned());
        mapped_file_one.insert("1".to_owned(), "file-one_1".to_owned());
        let complete_work_request_one = Request::new(CompleteWorkRequest {
            work_type: String::from("map"),
            files: mapped_file_one,
        });

        coordinate.complete_work(complete_work_request_one).await?;

        let state_guard = coordinate.state_guard.lock().unwrap();
        let part_0 = &state_guard.partitions[0];
        assert_eq!(1, part_0.files.len());
        assert_eq!(String::from("file-one_0"), part_0.files[0]);
        assert_eq!(0, part_0.id);
        assert_eq!(ReduceStatus::Ready, part_0.status);

        let part_1 = &state_guard.partitions[1];
        assert_eq!(1, part_1.files.len());
        assert_eq!(String::from("file-one_1"), part_1.files[0]);
        assert_eq!(1, part_1.id);
        assert_eq!(ReduceStatus::Ready, part_1.status);
        drop(state_guard); // need to release lock for subsequent request

        let mut mapped_file_two = HashMap::new();
        mapped_file_two.insert("0".to_owned(), "file-two_0".to_owned());
        mapped_file_two.insert("1".to_owned(), "file-two_1".to_owned());
        let complete_work_request_one = Request::new(CompleteWorkRequest {
            work_type: String::from("map"),
            files: mapped_file_two,
        });

        coordinate.complete_work(complete_work_request_one).await?;

        let state_guard = coordinate.state_guard.lock().unwrap();
        let part_0 = &state_guard.partitions[0];
        assert_eq!(2, part_0.files.len());
        assert_eq!(vec![String::from("file-one_0"), String::from("file-two_0")],
                        part_0.files);
        assert_eq!(0, part_0.id);
        assert_eq!(ReduceStatus::Ready, part_0.status);

        let part_1 = &state_guard.partitions[1];
        assert_eq!(2, part_1.files.len());
        assert_eq!(vec![String::from("file-one_1"), String::from("file-two_1")],
                        part_1.files);
        assert_eq!(1, part_1.id);
        assert_eq!(ReduceStatus::Ready, part_1.status);

        Ok(())
    }
}
