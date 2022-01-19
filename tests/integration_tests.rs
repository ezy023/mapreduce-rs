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

    // #[test]
    fn test_retrieve_files() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mr-integration-test")
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();



        runtime.spawn(async {
            let files = vec![String::from("file-two"),
                             String::from("file-one")];
            let state = State::new(1, files);
            let coordinate = Coordinate::new(state);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8888);

            Server::builder()
                .add_service(CoordinatorServer::new(coordinate))
                .serve(addr)
                .await
        });

        /* -------------------RPC CLIENT -------------------*/


        runtime.block_on(async {
            let mut client = CoordinatorClient::connect("http://localhost:8888").await.unwrap();

            let request = tonic::Request::new(GetWorkRequest {
                id: String::from("test-id"),
            });


            let response = client.get_work(request).await.unwrap();

            println!("RESPONSE_ONE={:?}", response);

            assert_eq!(response.get_ref().files.len(), 1);
            assert_eq!(response.get_ref().files[0], "file-one");

            let request_two = tonic::Request::new(GetWorkRequest {
                id: String::from("test-id"),
            });


            let response_two = client.get_work(request_two).await.unwrap();
            println!("RESPONSE_TWO={:?}", response);

            assert_eq!(response_two.get_ref().files.len(), 1);
            assert_eq!(response_two.get_ref().files[0], "file-two");
        });
    }

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
            // let files = vec![
            //     Path::new(&root_dir).join("tests/fixtures/short_two.txt").to_str().unwrap(),
            //     Path::new(&root_dir).join("tests/fixtures/short_one.txt").to_str().unwrap(),
            // ].iter().map(|s| String::from(*s)).collect();

            let state = State::new(1, files);
            let coordinate = Coordinate::new(state);
            // This port needs to be different than the port used in the concurrent test or else client connections in one of the test will fail
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7777);

            Server::builder()
                .add_service(CoordinatorServer::new(coordinate))
                .serve(addr)
                .await
        });

        /* -------------------WORKER -------------------*/

        let worker_two = Worker{
            id:"123".to_owned(),
            map_func: { |key, value| (value, String::from("1")) },
            reduce_func: { |key, values| {
                // println!("Key: {}", key);
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
            }},
            partitions: 1,
            field_split_func: {|c| !c.is_alphabetic() },
            partition_output_root: Path::new("/tmp/sequential-mr").to_owned(),
            coordinator_address: String::from("http://127.0.0.1:7777"),
        };

        runtime.block_on(worker_two.work_loop());

        // TODO read in 'mr-correct-wc.txt' and use it for comparison or test output
        // There should be two reduced output files because there are 2 partitions
        compare_files(&Path::new("/tmp/sequential-mr"))
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
            // let files = vec![
            //     Path::new(&root_dir).join("tests/fixtures/short_two.txt").to_str().unwrap(),
            //     Path::new(&root_dir).join("tests/fixtures/short_one.txt").to_str().unwrap(),
            // ].iter().map(|s| String::from(*s)).collect();

            let state = State::new(2, files);
            let coordinate = Coordinate::new(state);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8888);

            Server::builder()
                .add_service(CoordinatorServer::new(coordinate))
                .serve(addr)
                .await
        });

        /* -------------------WORKER -------------------*/

        let worker_one = Worker{
            id:"123".to_owned(),
            map_func: { |key, value| {
                if value == "parting" {
                    println!("SEEN \"parting\"");
                }
                (value, String::from("1"))
            }},
            reduce_func: { |key, values| {
                // println!("Key: {}", key);
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
            }},
            partitions: 2,
            field_split_func: {|c| !c.is_alphabetic() },
            partition_output_root: Path::new("/tmp/concurrent-mr").to_owned(),
            coordinator_address: String::from("http://127.0.0.1:8888"),
        };

        runtime.spawn(worker_one.work_loop());

        let worker_two = Worker{
            id:"123".to_owned(),
            map_func: { |key, value| {
                if value == "parting" {
                    println!("SEEN \"parting\"");
                }
                (value, String::from("1"))
            }},
            reduce_func: { |key, values| {
                // println!("Key: {}", key);
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
            }},
            partitions: 2,
            field_split_func: {|c| !c.is_alphabetic() },
            partition_output_root: Path::new("/tmp/concurrent-mr").to_owned(),
            coordinator_address: String::from("http://127.0.0.1:8888"),
        };

        runtime.block_on(worker_two.work_loop());


        // There should be two reduced output files because there are 2 partitions
        compare_files(&Path::new("/tmp/concurrent-mr"))
    }

    fn compare_files(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let root_dir = std::env::var("CARGO_MANIFEST_DIR").expect("$CARGO_MANIFEST_DIR");
        let expected_path = File::open(Path::new(&root_dir).join("mr-correct-wc.txt"))?;
        // let expected_path = File::open(Path::new(&root_dir).join("short-correct.txt"))?;
        // let got_path = File::open(Path::new(&root_dir).join("mr-correct-wc.txt"))?; // gut check
        // let got_path = File::open(Path::new(&root_dir).join("erik-reduce.txt"))?;
        let reduced_output = collect_reduce_files(path)?;
        compare_mr_files(BufReader::new(expected_path), reduced_output)
    }


    fn collect_reduce_files(dir: &Path) -> Result<impl BufRead, Box<dyn std::error::Error>> {
        let mut reduce_files = vec![];
        for file in std::fs::read_dir(dir)? {
            let file = file?;
            let path = file.path();
            println!("Collecting reduce file {}", path.display());
            if path.is_file() {
                if let Some(file_name) = path.file_name().unwrap().to_str() {
                    if file_name.contains("reduce_output") {
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

        // dbg!(&contents_buf);
        Ok(Cursor::new(contents_buf))
    }


    #[derive(Clone)]
    struct FileCmpError {
        count_diff: HashMap<String, (String, String)>,
        expected_remaining: HashMap<String, String>,
        got_remaining: HashMap<String, String>,
    }

    impl std::fmt::Debug for FileCmpError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "diff {}.\n expected_remaining: {}.\n got_remaining: {}.\n", self.count_diff.len(), self.expected_remaining.len(), self.got_remaining.len())
        }
    }

    impl std::fmt::Display for FileCmpError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "diff {}.\n expected_remaining: {}.\n got_remaining: {}.\n", self.count_diff.len(), self.expected_remaining.len(), self.got_remaining.len())
        }
    }

    impl std::error::Error for FileCmpError {}

    fn compare_mr_files(expected_reader: impl BufRead, got_reader: impl BufRead) -> Result<(), Box<dyn std::error::Error>> {
        let expected_map = read_to_hashmap(expected_reader)?;
        let got_map = read_to_hashmap(got_reader)?;

        let mut expected_copy = expected_map.clone(); // make a copy of what's expected to remove present keys from,
        let mut got_copy = got_map.clone();


        let mut count_differences_map: HashMap<String, (String, String)> = HashMap::new();

        for (k, v) in got_map.iter() {
            match expected_map.get(k) {
                Some(expected_val) => {
                    if v != expected_val {
                        println!("Key {}. expected: {}, got: {}", &k, &expected_val, &v);
                        count_differences_map.insert(k.clone(), (expected_val.clone(), v.clone()));
                    } else {
                        expected_copy.remove(k);
                        got_copy.remove(k);
                    }
                },
                None => {},
            }
        }

        if count_differences_map.len() > 0 ||
            expected_copy.len() > 0 ||
            got_copy.len() > 0 {
                return Err(Box::new(FileCmpError{
                    count_diff: count_differences_map,
                    expected_remaining: expected_copy,
                    got_remaining: got_copy,
                }));
            }
        Ok(())
    }

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
