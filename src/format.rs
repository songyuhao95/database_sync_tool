use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartMode {
    Auto,
    Gtid,
    Position,
}

impl FromStr for StartMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "gtid" => Ok(Self::Gtid),
            "position" | "pos" | "binlog" => Ok(Self::Position),
            _ => Err(format!(
                "unsupported start mode {value:?}; expected auto, gtid or position"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MysqlVersion {
    V5_7,
    V8_0,
    V8_4,
}

impl MysqlVersion {
    pub fn default_port(self) -> u16 {
        match self {
            Self::V5_7 => 33061,
            Self::V8_0 => 33062,
            Self::V8_4 => 33063,
        }
    }
    pub fn binlog_log(self) -> &'static str {
        match self {
            Self::V5_7 => "mysql-5.7-binlog.log",
            Self::V8_0 => "mysql-8.0-binlog.log",
            Self::V8_4 => "mysql-8.4-binlog.log",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::V5_7 => "mysql5.7",
            Self::V8_0 => "mysql8.0",
            Self::V8_4 => "mysql8.4",
        }
    }
}

impl FromStr for MysqlVersion {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "5.7" | "5_7" | "mysql5.7" | "mysql5_7" | "mysql-5.7" => Ok(Self::V5_7),
            "8.0" | "8_0" | "mysql8.0" | "mysql8_0" | "mysql-8.0" => Ok(Self::V8_0),
            "8.4" | "8_4" | "mysql8.4" | "mysql8_4" | "mysql-8.4" => Ok(Self::V8_4),
            _ => Err(format!(
                "unsupported MySQL version {value:?}; expected 5.7, 8.0 or 8.4"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Json,
    Sql(MysqlVersion),
}

impl OutputFormat {
    pub fn label(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Sql(target) => target.label(),
        }
    }
}
