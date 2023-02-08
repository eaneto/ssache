use std::{net::TcpStream, process::Command, thread::sleep, time::Duration};

#[test]
fn checks_if_connection_is_successful() {
    let mut cmd = match Command::new("target/debug/ssache").spawn() {
        Ok(cmd) => cmd,
        Err(_) => panic!("Unable to start ssache for testing"),
    };

    sleep(Duration::from_secs(1));

    let stream = TcpStream::connect("127.0.0.1:7777");
    assert_eq!(stream.is_ok(), true);

    cmd.kill().unwrap();
}
