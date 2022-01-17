A implementation of MapReduce written in Rust.

Based on the lab for [MIT 6.824](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)


### Running Integration Tests

```sh
$ cargo test --test integration_tests
```

To print output from test run add `-- --nocapture`

```sh
$ cargo test --test integration_tests -- --nocapture
```