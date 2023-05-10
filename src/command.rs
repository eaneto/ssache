use std::time::Duration;

use crate::errors::SsacheError;

use log::debug;

#[derive(Debug, PartialEq)]
pub enum Command {
    // GET key
    Get { key: String },
    // SET key value
    Set { key: String, value: String },
    // EXPIRE key time(in milliseconds)
    Expire { key: String, time: Duration },
    // INCR key
    Incr { key: String },
    // DECR key
    Decr { key: String },
    // SAVE
    Save,
    // LOAD
    Load,
    // QUIT
    Quit,
    // PING message
    Ping { message: String },
    Unknown,
}
const CRLF: &str = "\r\n";

pub fn parse_command(command_line: Vec<String>) -> Result<Command, SsacheError> {
    let command = command_line.get(0).unwrap();
    if command.eq(&String::from("GET")) {
        if let Some(key) = command_line.get(1) {
            Ok(Command::Get {
                key: key.to_string(),
            })
        } else {
            debug!("not enough parameters for GET command");
            let message = format!("-ERROR not enough parameters for GET{CRLF}");
            Err(SsacheError::NotEnoughParameters { message })
        }
    } else if command.eq(&String::from("SET")) {
        if let (Some(key), Some(_)) = (command_line.get(1), command_line.get(2)) {
            Ok(Command::Set {
                key: key.to_string(),
                value: command_line[2..].join(" "),
            })
        } else {
            debug!("not enough parameters for SET command");
            let message = format!("-ERROR not enough parameters for SET{CRLF}");
            Err(SsacheError::NotEnoughParameters { message })
        }
    } else if command.eq(&String::from("EXPIRE")) {
        if let (Some(key), Some(time)) = (command_line.get(1), command_line.get(2)) {
            // If the expiration time is unparseable use 0, this value
            // is ignored.
            let time = time.parse::<u64>().unwrap_or(0);
            Ok(Command::Expire {
                key: key.to_string(),
                time: Duration::from_millis(time),
            })
        } else {
            debug!("not enough parameters for EXPIRE command");
            let message = format!("-ERROR not enough parameters for EXPIRE{CRLF}");
            Err(SsacheError::NotEnoughParameters { message })
        }
    } else if command.eq(&String::from("SAVE")) {
        Ok(Command::Save)
    } else if command.eq(&String::from("INCR")) {
        if let Some(key) = command_line.get(1) {
            Ok(Command::Incr {
                key: key.to_string(),
            })
        } else {
            debug!("not enough parameters for INCR command");
            let message = format!("-ERROR not enough parameters for INCR{CRLF}");
            Err(SsacheError::NotEnoughParameters { message })
        }
    } else if command.eq(&String::from("DECR")) {
        if let Some(key) = command_line.get(1) {
            Ok(Command::Decr {
                key: key.to_string(),
            })
        } else {
            debug!("not enough parameters for DECR command");
            let message = format!("-ERROR not enough parameters for DECR{CRLF}");
            Err(SsacheError::NotEnoughParameters { message })
        }
    } else if command.eq(&String::from("LOAD")) {
        Ok(Command::Load)
    } else if command.eq(&String::from("QUIT")) {
        Ok(Command::Quit)
    } else if command.eq(&String::from("PING")) {
        let value = command_line[1..].join(" ");
        Ok(Command::Ping { message: value })
    } else {
        Ok(Command::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_unkown_command() {
        let command_line = vec!["UNKNOWN".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Command::Unknown);
    }

    #[test]
    fn parse_ping_with_no_arguments() {
        let command_line = vec!["PING".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Command::Ping {
                message: "".to_string()
            }
        );
    }

    #[test]
    fn parse_ping_with_custom_message() {
        let command_line = vec!["PING".to_string(), "some message".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Command::Ping {
                message: "some message".to_string()
            }
        );
    }

    #[test]
    fn parse_quit_command() {
        let command_line = vec!["QUIT".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Command::Quit);
    }

    #[test]
    fn parse_save_command() {
        let command_line = vec!["SAVE".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Command::Save);
    }

    #[test]
    fn parse_load_command() {
        let command_line = vec!["LOAD".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Command::Load);
    }

    #[test]
    fn parse_get_command_without_enough_arguments() {
        let command_line = vec!["GET".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_err());
    }

    #[test]
    fn parse_get_command_with_enough_arguments() {
        let command_line = vec!["GET".to_string(), "key".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Command::Get {
                key: "key".to_string()
            }
        );
    }

    #[test]
    fn parse_set_command_with_no_arguments() {
        let command_line = vec!["SET".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_err());
    }

    #[test]
    fn parse_set_command_with_only_the_key_arguments() {
        let command_line = vec!["SET".to_string(), "key".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_err());
    }

    #[test]
    fn parse_set_command_with_enough_arguments() {
        let command_line = vec!["SET".to_string(), "key".to_string(), "value".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Command::Set {
                key: "key".to_string(),
                value: String::from("value"),
            }
        );
    }

    #[test]
    fn parse_set_command_with_spaces_on_value() {
        let command_line = vec![
            "SET".to_string(),
            "key".to_string(),
            "value with spaces".to_string(),
        ];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Command::Set {
                key: "key".to_string(),
                value: "value with spaces".to_string(),
            }
        );
    }

    #[test]
    fn parse_expire_command_with_no_arguments() {
        let command_line = vec!["EXPIRE".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_err());
    }

    #[test]
    fn parse_expire_command_with_only_the_key_arguments() {
        let command_line = vec!["EXPIRE".to_string(), "key".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_err());
    }

    #[test]
    fn parse_expire_command_with_enough_arguments() {
        let command_line = vec!["EXPIRE".to_string(), "key".to_string(), "1000".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Command::Expire {
                key: "key".to_string(),
                time: Duration::from_millis(1000)
            }
        );
    }

    #[test]
    fn parse_incr_command_without_enough_arguments() {
        let command_line = vec!["INCR".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_err());
    }

    #[test]
    fn parse_incr_command_with_enough_arguments() {
        let command_line = vec!["INCR".to_string(), "key".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Command::Incr {
                key: "key".to_string()
            }
        );
    }

    #[test]
    fn parse_decr_command_without_enough_arguments() {
        let command_line = vec!["DECR".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_err());
    }

    #[test]
    fn parse_decr_command_with_enough_arguments() {
        let command_line = vec!["DECR".to_string(), "key".to_string()];

        let result = parse_command(command_line);

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Command::Decr {
                key: "key".to_string()
            }
        );
    }
}
