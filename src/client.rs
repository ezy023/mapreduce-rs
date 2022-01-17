use std::hash::{Hash, Hasher};
use std::io::{self, Read, Write, BufReader, BufRead, BufWriter};
use std::fs::File;
use std::path::PathBuf;
use std::collections::{
    hash_map::DefaultHasher,
    HashMap,
};
use std::write;
use crate::coordinator_client::CoordinatorClient;
use crate::{GetWorkRequest, CompleteWorkRequest};


pub struct Worker<M, R>
where
    M: Fn(String, String) -> (String, String),
    R: Fn(&String, &Vec<String>) -> i32,
{
    pub id: String,
    // map_func takes a file path and returns a hashmap of results which the imple writes to disk?
    // output is the input val and its transformed val?
    pub map_func: M,
    // reduce_fun is passed files for its hashed results and then performs the reduction and returns a map of results?
    pub reduce_func: R,
    pub partitions: u32, // each partition needs to be reduced
    pub field_split_func: fn(char) -> bool,
    pub partition_output_root: PathBuf,
    pub coordinator_address: String,
}

impl<M, R> Worker<M, R>
where
    M: Fn(String, String) -> (String, String),
    R: Fn(&String, &Vec<String>) -> i32,
{
    fn hash(s: &String) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    pub async fn work_loop(self) {
        /* retry connection until one is established, this should be limited to a set of retries */
        let mut client =
            loop {
                if let Ok(client) = CoordinatorClient::connect(self.coordinator_address.clone()).await {
                    break client;
                }
        };

        loop {
            let request = tonic::Request::new(GetWorkRequest{id: self.id.clone()});
            if let Ok(mut response) = client.get_work(request).await {
                let response = response.get_mut();
                match response.work_type.as_str() {
                    "map" => {
                        let filename = response.files.pop().unwrap();
                        match self.map(filename.clone()) {
                            Ok(mapped_paths) => {
                                let complete_request = tonic::Request::new(CompleteWorkRequest{
                                    work_type: String::from("map"),
                                    original_file: filename,
                                    files: mapped_paths,
                                });
                                println!("Completing MAP work");
                                // TODO need to handle retries and failure of the server to ack the completion
                                if let Err(e) = client.complete_work(complete_request).await {
                                    println!("ERROR complete map {:?}", e);
                                }
                            },
                            Err(e) => println!("ERROR {:?}", e),
                        }
                    },
                    "reduce" => {
                        println!("REDUCING");
                        let files = response.files.iter()
                            .map(|f| Box::new(f.into()))
                            .collect();
                        dbg!(&files);
                        match self.reduce(response.partition, files) {
                            Ok(reduce_output_file) => {
                                let mut reduced = HashMap::new();
                                reduced.insert(response.partition.to_string(), reduce_output_file);
                                let complete_request = tonic::Request::new(CompleteWorkRequest{
                                    work_type: String::from("reduce"),
                                    original_file: String::new(), // TODO this is a hack for tracking the map file status
                                    files: reduced,
                                });
                                if let Err(e) = client.complete_work(complete_request).await {
                                    println!("ERROR complete reduce {:?}", e);
                                }
                            },
                            Err(e) => println!("REDUCE ERROR {:?}", e),
                        }
                    },
                    // TODO loop HACK
                    "done" => {
                        println!("DONE");
                        return;
                    },
                    _ => panic!("Unexpected work type {}", response.work_type),
                }
            } else {
                println!("received error, looping");
            }
        }
    }

    // The results need to be returned so they can be partitioned and written out
    fn map(&self, filepath: String) -> Result<HashMap<String, String>, io::Error> {
        // open file, split values, group to map, pass to func
        let mut file = File::open(&filepath)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let results = self.map_contents(&filepath, contents); // filepath represents document_id

        // write out partitions, need to send partition filepaths back to coordinator
        let mut output_files = HashMap::new();
        for (partition, values) in results.iter() {
            let partition_file = self.partition_output_root.join(format!("{}_{}", filepath, partition));
            let mut file = File::create(&partition_file)?;
            // dbg!(&file);
            for (token, value) in values.iter() {
                // println!("Writing {},{} to file {}", &token, &value, &partition);
                write!(file, "{},{}\n", token, value)?;
            }
            output_files.insert(partition.to_string(), String::from(partition_file.to_str().unwrap()));
        }

        // these results need to be partitioned and written
        Ok(output_files)
    }

    // filepath is more of a documentId
    fn map_contents(&self, filepath: &String, contents: String) -> HashMap<u64, Vec<(String, String)>> {
        let mut map = HashMap::<u64, Vec<(String, String)>>::new();
        for token in contents.split(self.field_split_func).filter(|s| !s.is_empty()) { // 'filter' removes empty strings that result from contiguous separators, see 'split' docs
            let string_token = String::from(token);
            let partition = Self::hash(&string_token) % self.partitions as u64;
            let (tok, val) = (self.map_func)(filepath.clone(), string_token);
            if let Some(v) = map.get_mut(&partition) {
                (*v).push((tok, val));
            } else {
                map.insert(partition, vec![(tok, val)]);
            }
        }
        map
    }

    fn reduce(&self, partition: u32, filepaths: Vec<Box<PathBuf>>) -> Result<String, io::Error> {
        // step 1. pre-aggregate the results from map
        let mut aggregation : HashMap<String, Vec<String>> = HashMap::new();
        for filepath in filepaths.iter() {
            let file = File::open(filepath.as_ref())?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line.unwrap();
                let parts : Vec<&str> = line.split(',').collect();
                if let Some(val) = aggregation.get_mut(parts[0]) {
                    (*val).push(String::from(parts[1]));
                } else {
                    aggregation.insert(String::from(parts[0]), vec![String::from(parts[1])]);
                }
            }
        }

        // step 2. apply reduce function
        let mut results : HashMap<String, i32> = HashMap::new();
        for (key, vals) in aggregation.iter() {
            let result = (self.reduce_func)(key, vals);
            results.insert(key.clone(), result);
        }


        // step 3. write output
        let output_filepath = self.partition_output_root.join(format!("reduce_output_{}", partition));
        let output_file = File::create(&output_filepath)?;
        let mut writer = BufWriter::new(output_file);
        for (key, val) in results.iter() {
            if let Err(e) = write!(writer, "{} {}\n", key, val) {
                println!("Error writing {},{}. {:?}", key, val, e);
            }
        }
        writer.flush()?;

        Ok(String::from(output_filepath.to_str().unwrap())) // TODO use paths
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_map_contents() {
        let worker = Worker {
            id: "123".to_owned(),
            map_func: { |key, value| (value, String::from("1")) },
            reduce_func: { |key, values| unimplemented!() },
            partitions: 1,
            field_split_func: char::is_whitespace,
            partition_output_root: Path::new("/tmp").to_owned(),
            coordinator_address: String::new(),
        };

        let contents = String::from("words to test with");
        let results = worker.map_contents(&String::from("/tmp"), contents);
        let expected = HashMap::from([
            (0, vec![
                ("words".to_owned(), "1".to_owned()),
                ("to".to_owned(), "1".to_owned()),
                ("test".to_owned(), "1".to_owned()),
                ("with".to_owned(), "1".to_owned())
            ]),
        ]);

        assert_eq!(expected, results);
    }

    #[test]
    fn test_map() -> Result<(), io::Error> {
        let worker = Worker {
            id: "123".to_owned(),
            map_func: { |key, value| (value, String::from("1")) },
            reduce_func: { |key, values| unimplemented!() },
            partitions: 1,
            field_split_func: char::is_whitespace,
            partition_output_root: Path::new("/tmp").to_owned(),
            coordinator_address: String::new(),
        };
        let input_path = worker.partition_output_root.join("test_input");
        let mut input = File::create(input_path.clone())?;
        write!(input, "one two two three three three");

        let mut intermediate_results = worker.map(input_path.to_str().unwrap().to_owned())?;

        assert_eq!(1, intermediate_results.len());

        let (_, intermediate_path) = intermediate_results.iter().nth(0).unwrap();
        let mut intermediate_data = String::new();
        let intermediate_file = File::open(intermediate_path)?.read_to_string(&mut intermediate_data);
        let expected = r#"one,1
two,1
two,1
three,1
three,1
three,1
"#;
        assert_eq!(expected, intermediate_data);


        // TODO cleanup
        Ok(())
    }


    #[test]
    fn test_reduce() -> Result<(), io::Error> {
        let worker = Worker {
            id: "123".to_owned(),
            map_func: { |key, value| unimplemented!() },
            reduce_func: { |key, values| {
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
            }
            }, // the function should receive input like 'key vec[1, 1, 1, 1]' or the like, it should receive the full set of value for the key
            partitions: 1,
            field_split_func: char::is_whitespace,
            partition_output_root: Path::new("/tmp").to_owned(),
            coordinator_address: String::new(),
        };

        let input = r#"one,1
two,1
two,1
three,1
three,1
three,1
"#;

        let path = worker.partition_output_root.join("test_partition_file");
        let mut input_file = File::create(path.as_path())?;
        write!(input_file, "{}", input);

        let output_filepath = worker.reduce(0, vec![Box::new(path)]).unwrap();

        let mut output_file = File::open(output_filepath).unwrap();
        let mut output_contents = String::new();
        output_file.read_to_string(&mut output_contents)?;
        dbg!(&output_contents);
        output_contents.trim().split('\n')
            .map(|line| {
                let entry_vec: Vec<&str> = line.split_whitespace().collect();
                (String::from(entry_vec[0]), entry_vec[1].parse::<i32>().unwrap())
            })
            .for_each(|(key, count)| {
                match key.as_str() {
                    "one" => assert_eq!(1, count, "count for 'one' is {}", count),
                    "two" => assert_eq!(2, count, "count for 'two' is {}", count),
                    "three" => assert_eq!(3, count, "count for 'three' is {}", count),
                    _ => panic!("Unknown key {}", key),
                }
            });

        Ok(())
    }
}
