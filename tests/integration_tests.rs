#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    use std::path::Path;
    use std::sync::Mutex;
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
    fn test_map_reduce() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("mr-integration-test")
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();

        /* -------------------COORDINATOR -------------------*/

        runtime.spawn(async {
            let files = vec![
                "/Users/erik/code/rust/mapreduce/tests/fixtures/pg-being_ernest.txt",
                "/Users/erik/code/rust/mapreduce/tests/fixtures/pg-dorian_gray.txt",
                // "/Users/erik/code/rust/mapreduce/tests/fixtures/pg-frankenstein.txt",
                // "/Users/erik/code/rust/mapreduce/tests/fixtures/pg-grimm.txt",
                // "/Users/erik/code/rust/mapreduce/tests/fixtures/pg-huckleberry_finn.txt",
                // "/Users/erik/code/rust/mapreduce/tests/fixtures/pg-metamorphosis.txt",
                // "/Users/erik/code/rust/mapreduce/tests/fixtures/pg-sherlock_holmes.txt",
                // "/Users/erik/code/rust/mapreduce/tests/fixtures/pg-tom_sawyer.txt",
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

        let worker_one = Worker{
            id:"123".to_owned(),
            map_func: { |key, value| (value, String::from("1")) },
            reduce_func: { |key, values| {
                println!("Key: {}", key);
                dbg!(&values);
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
            }},
            partitions: 2,
            field_split_func: {|c| c.is_whitespace() || [',', '?', '.', '"', ':', ';', ']', '[', ')', '('].contains(&c) },
            partition_output_root: Path::new("/tmp").to_owned(),
            coordinator_address: String::from("http://127.0.0.1:8888"),
        };

        runtime.spawn(worker_one.work_loop());

        let worker_two = Worker{
            id:"123".to_owned(),
            map_func: { |key, value| (value, String::from("1")) },
            reduce_func: { |key, values| {
                println!("Key: {}", key);
                dbg!(&values);
                values.iter()
                    .map(|v| v.parse::<i32>().unwrap())
                    .fold(0, |accum, item| accum + item)
            }},
            partitions: 2,
            field_split_func: {|c| c.is_whitespace() || [',', '?', '.', '"', ':', ';'].contains(&c) },
            partition_output_root: Path::new("/tmp").to_owned(),
            coordinator_address: String::from("http://127.0.0.1:8888"),
        };

        runtime.block_on(worker_two.work_loop());

    }
}
