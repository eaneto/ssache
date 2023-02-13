use log::debug;

#[derive(Debug, PartialEq)]
pub enum Command {
    // GET key
    Get { key: String },
    // SET key value
    Set { key: String, value: String },
    Quit,
    // PING message
    Ping { message: String },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct NotEnoughParametersError {
    pub message: String,
}

const CRLF: &str = "\r\n";

pub fn parse_command(command_line: Vec<String>) -> Result<Command, NotEnoughParametersError> {
    let command = command_line.get(0).unwrap();
    if command.eq(&String::from("GET")) {
        if let Some(key) = command_line.get(1) {
            Ok(Command::Get {
                key: key.to_string(),
            })
        } else {
            debug!("not enough parameters for GET command");
            let message = format!("-ERROR not enough parameters for GET{CRLF}");
            Err(NotEnoughParametersError { message })
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
            Err(NotEnoughParametersError { message })
        }
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
        let mut command_line = Vec::new();
        command_line.push("UNKOWN".to_string());

        let result = parse_command(command_line);

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), Command::Unknown);
    }

    #[test]
    fn parse_quit_command() {
        let mut command_line = Vec::new();
        command_line.push("QUIT".to_string());

        let result = parse_command(command_line);

        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), Command::Quit);
    }

    #[test]
    fn parse_get_command_without_enough_arguments() {
        let mut command_line = Vec::new();
        command_line.push("GET".to_string());

        let result = parse_command(command_line);

        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn parse_get_command_with_enough_arguments() {
        let mut command_line = Vec::new();
        command_line.push("GET".to_string());
        command_line.push("key".to_string());

        let result = parse_command(command_line);

        assert_eq!(result.is_ok(), true);
        assert_eq!(
            result.unwrap(),
            Command::Get {
                key: "key".to_string()
            }
        );
    }

    #[test]
    fn parse_set_command_with_no_arguments() {
        let mut command_line = Vec::new();
        command_line.push("SET".to_string());

        let result = parse_command(command_line);

        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn parse_set_command_with_only_the_key_arguments() {
        let mut command_line = Vec::new();
        command_line.push("SET".to_string());
        command_line.push("key".to_string());

        let result = parse_command(command_line);

        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn parse_set_command_with_enough_arguments() {
        let mut command_line = Vec::new();
        command_line.push("SET".to_string());
        command_line.push("key".to_string());
        command_line.push("value".to_string());

        let result = parse_command(command_line);

        assert_eq!(result.is_ok(), true);
        assert_eq!(
            result.unwrap(),
            Command::Set {
                key: "key".to_string(),
                value: "value".to_string(),
            }
        );
    }
}
