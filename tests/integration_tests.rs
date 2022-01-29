#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    use std::path::Path;
    use std::sync::Mutex;
    use std::io::{BufRead, BufReader, Cursor, Read};
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::PathBuf;
    use tonic::transport::Server;
    use tokio::runtime::Builder;
    use mapreduce::{
        CoordinatorServer, // generated from rpc proto
        coordinator_client::CoordinatorClient, // generated from rpc proto
        State, Coordinate, GetWorkRequest,
        client::Worker
    };

    #[test]
    fn test_map_reduce_sequential() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mr-integration-test")
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();

        /* -------------------COORDINATOR -------------------*/

        runtime.spawn(async {
            let root_dir = std::env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
            let files = vec![
                Path::new(&root_dir).join("tests/fixtures/pg-being_ernest.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-dorian_gray.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-frankenstein.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-grimm.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-huckleberry_finn.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-metamorphosis.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-sherlock_holmes.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-tom_sawyer.txt").to_str().unwrap(),
            ].iter().map(|s| String::from(*s)).collect();

            let state = State::new(1, files);
            let coordinate = Coordinate::new(state);
            // This port needs to be different than the port used in the concurrent test or else client connections in one of the test will fail
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7777);

            Server::builder()
                .add_service(CoordinatorServer::new(coordinate))
                .serve(addr)
                .await;
        });

        /* -------------------WORKER -------------------*/

        let partition_root = Path::new("/tmp/sequential-mr");

        let worker_two = Worker{
            id:"123".to_owned(),
            map_func: { |_key, value| (value, String::from("1")) },
            reduce_func: { |_key, values| {
                // println!("Key: {}", key);
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
                    .to_string()
            }},
            partitions: 1,
            field_split_func: {|c| !c.is_alphabetic() },
            partition_output_root: partition_root.to_path_buf(),
            coordinator_address: String::from("http://127.0.0.1:7777"),
        };

        runtime.block_on(worker_two.work_loop());

        let root_dir = std::env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
        let expected_path = Path::new(&root_dir).join("tests/fixtures/mr-correct-wc.txt");
        let result = compare_files(expected_path, partition_root, wc_entry_compare);
        if let Err(e) = clean_up_files(&partition_root) {
            println!("Failed removing test files from {}. {:?}", partition_root.display(), e);
        }

        result
    }

    #[test]
    fn test_map_reduce_concurrent() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mr-integration-test")
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();

        /* -------------------COORDINATOR -------------------*/

        runtime.spawn(async {
            let root_dir = std::env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
            let files = vec![
                Path::new(&root_dir).join("tests/fixtures/pg-being_ernest.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-dorian_gray.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-frankenstein.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-grimm.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-huckleberry_finn.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-metamorphosis.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-sherlock_holmes.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-tom_sawyer.txt").to_str().unwrap(),
            ].iter().map(|s| String::from(*s)).collect();

            let state = State::new(2, files);
            let coordinate = Coordinate::new(state);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8888);

            Server::builder()
                .add_service(CoordinatorServer::new(coordinate))
                .serve(addr)
                .await
        });

        /* -------------------WORKER -------------------*/

        let partition_root = Path::new("/tmp/concurrent-mr");

        let worker_one = Worker{
            id:"123".to_owned(),
            map_func: { |_key, value| {
                (value, String::from("1"))
            }},
            reduce_func: { |_key, values| {
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
                    .to_string()
            }},
            partitions: 2,
            field_split_func: {|c| !c.is_alphabetic() },
            partition_output_root: partition_root.to_path_buf(),
            coordinator_address: String::from("http://127.0.0.1:8888"),
        };

        runtime.spawn(worker_one.work_loop());

        let worker_two = Worker{
            id:"123".to_owned(),
            map_func: { |_key, value| {
                (value, String::from("1"))
            }},
            reduce_func: { |_key, values| {
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
                    .to_string()
            }},
            partitions: 2,
            field_split_func: {|c| !c.is_alphabetic() },
            partition_output_root: partition_root.to_path_buf(),
            coordinator_address: String::from("http://127.0.0.1:8888"),
        };

        runtime.block_on(worker_two.work_loop());

        let root_dir = std::env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
        let expected_path = Path::new(&root_dir).join("tests/fixtures/mr-correct-wc.txt");
        // There should be two reduced output files because there are 2 partitions
        let result = compare_files(expected_path, partition_root, wc_entry_compare);
        if let Err(e) = clean_up_files(&partition_root) {
            println!("Failed removing test files from {}. {:?}", partition_root.display(), e);
        }
        result
    }

    #[test]
    fn test_map_reduce_indexer() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mr-integration-test")
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();

        /* -------------------COORDINATOR -------------------*/

        runtime.spawn(async {
            let root_dir = std::env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
            let files = vec![
                Path::new(&root_dir).join("tests/fixtures/pg-being_ernest.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-dorian_gray.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-frankenstein.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-grimm.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-huckleberry_finn.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-metamorphosis.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-sherlock_holmes.txt").to_str().unwrap(),
                Path::new(&root_dir).join("tests/fixtures/pg-tom_sawyer.txt").to_str().unwrap(),
            ].iter().map(|s| String::from(*s)).collect();

            let state = State::new(2, files);
            let coordinate = Coordinate::new(state);
            // This port needs to be different than the port used in the concurrent test or else client connections in one of the test will fail
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6666);

            Server::builder()
                .add_service(CoordinatorServer::new(coordinate))
                .serve(addr)
                .await;
        });

        let partition_root = Path::new("/tmp/mr-indexer");

        let worker_one = Worker{
            id: String::from("indexer-worker"),
            map_func: { |document, value| {
                let filename = PathBuf::from(document).file_name().unwrap().to_str().unwrap().to_owned();
                (value, filename)
            }},
            reduce_func: { |key, values| {
                let mut set = std::collections::HashSet::new();
                for val in values.into_iter() {
                    set.insert(val.clone());
                }
                let set_values = set.into_iter().collect::<Vec<String>>();
                let v = set_values.join(",");
                format!("{} {v}", set_values.len())
            }},
            partitions: 2,
            field_split_func: {|c| !c.is_alphabetic() },
            partition_output_root: partition_root.to_path_buf(),
            coordinator_address: String::from("http://127.0.0.1:6666"),
        };

        runtime.block_on(worker_one.work_loop());

        let root_dir = std::env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
        let expected_path = Path::new(&root_dir).join("tests/fixtures/mr-correct-indexer.txt");
        let result = compare_files(expected_path, partition_root, indexer_entry_compare);
        if let Err(e) = clean_up_files(partition_root) {
            println!("Failed removing test files from {}. {:?}", partition_root.display(), e);
        }
        result
    }

    fn wc_entry_compare(expected: &String, got: &String) -> bool {
        expected == got
    }

    fn indexer_entry_compare(expected: &String, got: &String) -> bool {
        let exp_parts: Vec<String> = expected.split_whitespace().map(String::from).collect();
        let got_parts: Vec<String> = got.split_whitespace().map(String::from).collect();

        if got_parts.len() < 2 || got_parts[0] != exp_parts[0] {
            return false;
        } else {
            let mut exp_idx_entries: Vec<String> = exp_parts[1].split(',')
                .map(|i| i.trim_start_matches("../").to_owned())
                .collect();
            let mut got_idx_entries: Vec<String> = got_parts[1].split(',').map(String::from).collect();
            if exp_idx_entries.len() != got_idx_entries.len() {
                return false;
            }

            for item in exp_idx_entries.iter() {
                if !got_idx_entries.contains(item) {
                    return false;
                }
            }
        }
        true
    }

    fn compare_files<E, P>(expected_path: E, partition_root: P, comp_func: fn(&String, &String) -> bool) -> Result<(), Box<dyn std::error::Error>>
    where
        E: AsRef<Path>,
        P: AsRef<Path>,
    {
        let reduced_output_reader = collect_reduce_files(partition_root)?;
        let expected_file = File::open(expected_path)?;

        let expected_map = read_to_hashmap(BufReader::new(expected_file))?;
        let got_map = read_to_hashmap(reduced_output_reader)?;

        if expected_map.len() != got_map.len() {
            return Err(Box::new(FileCmpError{
                diff: HashMap::new(),
                message: String::from("expected size not equal to got size"),
            }));
        }

        let mut diff = HashMap::new();

        for(k, v) in expected_map.iter() {
            match got_map.get(k) {
                Some(got_val) => {
                    if ! comp_func(v, got_val) {
                        diff.insert(k.clone(), (v.clone(), got_val.clone()));
                    }
                },
                None => {
                    diff.insert(k.clone(), (v.clone(), String::new()));
                },
            }
        }

        if diff.len() > 0 {
            return Err(Box::new(FileCmpError{
                diff: diff,
                message: String::from("contents not equal"),
            }));
        }

        Ok(())
    }

    fn clean_up_files<P: AsRef<Path>>(path: P) -> Result<(), Box<dyn std::error::Error>> {
        for file in std::fs::read_dir(path).unwrap() {
            let path = file?.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().unwrap().to_str() {
                    if file_name.contains("reduce_output") {
                        std::fs::remove_file(path)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn collect_reduce_files<P: AsRef<Path>>(dir: P) -> Result<impl BufRead, Box<dyn std::error::Error>> {
        let mut reduce_files = vec![];
        for file in std::fs::read_dir(dir)? {
            let file = file?;
            let path = file.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().unwrap().to_str() {
                    if file_name.contains("reduce_output") {
                        println!("Collecting reduce file {}", path.display());
                        reduce_files.push(path);
                    }
                }
            }
        }
        let mut contents_buf = Vec::new();
        let mut tmp_buf = Vec::new();
        for file in reduce_files.iter() {
            let mut file = File::open(file)?;
            file.read_to_end(&mut tmp_buf)?;
            contents_buf.append(&mut tmp_buf);
        }

        Ok(Cursor::new(contents_buf))
    }


    #[derive(Clone)]
    struct FileCmpError {
        diff: HashMap<String, (String, String)>,
        message: String,
    }

    impl std::fmt::Debug for FileCmpError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}\ndiff {}.", self.message, self.diff.len())
        }
    }

    impl std::fmt::Display for FileCmpError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}\n diff {}.\n", self.message, self.diff.len())
        }
    }

    impl std::error::Error for FileCmpError {}


    fn read_to_hashmap<T: BufRead>(reader: T) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
        let hashmap = reader.lines()
            .map(|l| l.unwrap()) // the Lines iterator returns an io::Result<String>
            .map(|l| {
                let v: Vec<&str> = l.splitn(2, ' ').collect();
                if v.len() != 2 {
                    return (String::new(), v[0].to_owned());
                }
                (v[0].to_owned(), v[1].to_owned()) // panic if the split has less than 2 elements
            })
            .fold(HashMap::new(), |mut acc, item| {
                acc.insert(item.0, item.1);
                acc
            });

        Ok(hashmap)
    }
}
