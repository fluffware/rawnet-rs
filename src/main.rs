use clap::Parser;
use rustyline_async::{Readline, ReadlineError};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::process::ExitCode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

mod parser {
    use nom::branch::alt;
    use nom::bytes::complete::{escaped_transform, is_not, tag};
    use nom::character::complete::{self, satisfy, space0};
    use nom::combinator::{map, map_res, value};
    use nom::error::FromExternalError;
    use nom::multi::{fold_many0, fold_many_m_n};
    use nom::sequence::{delimited, preceded};
    use nom::IResult;
    use std::fmt::{self, Display, Formatter};
    use std::num::ParseIntError;

    #[derive(Debug, PartialEq)]
    pub enum ParseErrorKind {
        InvalidHexNumber,
        Nom(nom::error::ErrorKind),
    }
    impl Display for ParseErrorKind {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
            match self {
                ParseErrorKind::InvalidHexNumber => {
                    write!(f, "Invalid hex number")
                }
                ParseErrorKind::Nom(err) => {
                    write!(f, "{}", err.description())
                }
            }
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct ParseError<'a> {
        input: &'a str,
        kind: ParseErrorKind,
    }
    impl std::error::Error for ParseError<'_> {}

    impl Display for ParseError<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
            Display::fmt(&self.kind, f)
        }
    }

    impl<'a> nom::error::ParseError<&'a str> for ParseError<'a> {
        fn from_error_kind(input: &'a str, kind: nom::error::ErrorKind) -> Self {
            ParseError {
                input,
                kind: ParseErrorKind::Nom(kind),
            }
        }

        fn append(_input: &'a str, _kind: nom::error::ErrorKind, other: Self) -> Self {
            other
        }
    }

    impl<'a> FromExternalError<&'a str, ParseIntError> for ParseError<'a> {
        fn from_external_error(
            input: &'a str,
            _kind: nom::error::ErrorKind,
            _int_err: ParseIntError,
        ) -> Self {
            ParseError {
                input,
                kind: ParseErrorKind::InvalidHexNumber,
            }
        }
    }

    impl<'a> FromExternalError<&'a str, ParseErrorKind> for ParseError<'a> {
        fn from_external_error(
            input: &'a str,
            _: nom::error::ErrorKind,
            kind: ParseErrorKind,
        ) -> Self {
            ParseError { input, kind }
        }
    }

    type ParseResult<'a, T> = IResult<&'a str, T, ParseError<'a>>;

    fn from_hex(input: &str) -> Result<u8, std::num::ParseIntError> {
        u8::from_str_radix(input, 16)
    }

    fn hex_byte(input: &str) -> ParseResult<u8> {
        map_res(
            fold_many_m_n(
                1,
                2,
                satisfy(|c| c.is_ascii_hexdigit()),
                || 0u32,
                |a, d| a * 16 + d.to_digit(16).unwrap(),
            ),
            |v| {
                if v >= 256 {
                    Err(ParseErrorKind::InvalidHexNumber)
                } else {
                    Ok(v as u8)
                }
            },
        )(input)
    }

    fn string(input: &str) -> ParseResult<String> {
        delimited(
            complete::char('"'),
            escaped_transform(
                is_not("\\\""),
                '\\',
                alt((
                    value("\\", tag("\\")),
                    value("\"", tag("\"")),
                    value("\n", tag("n")),
                )),
            ),
            complete::char('"'),
        )(input)
    }

    pub fn byte_values(input: &str) -> ParseResult<Vec<u8>> {
        fold_many0(
            preceded(
                space0,
                alt((
                    map(hex_byte, |b| [b].to_vec()),
                    map(string, |v| v.as_bytes().to_vec()),
                )),
            ),
            || Vec::new(),
            |mut v, mut b| {
                v.append(&mut b);
                v
            },
        )(input)
    }
}

const BYTES_PER_ROW: usize = 16;
fn dump_bytes<W: Write>(w: &mut W, prefix: &str, bytes: &[u8]) -> Result<(), std::io::Error> {
    for row in bytes.chunks(BYTES_PER_ROW) {
        write!(w, "{prefix}")?;
        for b in row {
            write!(w, " {:02x}", b);
        }
        for _ in row.len()..BYTES_PER_ROW {
            write!(w, "   ");
        }
        write!(w," ")?;
        for b in row {
            write!(
                w,
                "{}",
                if *b >= 32u8 && *b <= 127u8 {
                    *b as char
                } else {
                    '.'
                }
            );
        }
        writeln!(w, "");
    }
    Ok(())
}

enum NetMessage {
    Data(Vec<u8>),
    NewConnection(SocketAddr),
    Disconnected(SocketAddr),
    Warning(String),
}

enum NetCmd {
    Send(Vec<u8>),
}

type DynResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn tcp_server(
    bind: SocketAddr,
    mut cmd: Receiver<NetCmd>,
    msg: Sender<NetMessage>,
) -> DynResult<()> {
    let listener = TcpListener::bind(bind).await?;
    let mut buffer = [0u8; 1024];
    loop {
        let ret = listener.accept().await;
        let mut stream;
        let remote;

        match ret {
            Ok((s, r)) => {
                stream = s;
                remote = r;
                let _ = msg.send(NetMessage::NewConnection(r)).await;
            }
            Err(e) => {
                let _ = msg
                    .send(NetMessage::Warning(format!("accept failed: {}", e)))
                    .await;
                continue;
            }
        }
        let mut running = true;
        while running {
            tokio::select! {
                ret = stream.read(&mut buffer) => {
                    match ret {
                        Ok(l) => {
                            if l > 0 {
                                let _ = msg.send(NetMessage::Data(buffer[..l].to_vec())).await;
                            } else {
                                let _ = msg.send(NetMessage::Disconnected(remote)).await;
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = msg.send(NetMessage::Warning(format!("read failed: {}",e)))
                                .await;
                        }
                    }
                }
                res = cmd.recv() => {
                    match res {
                        Some(cmd) => {
                            match cmd {
                                NetCmd::Send(bytes) => {
                                    stream.write(&bytes).await;
                                    stream.flush().await;
                                }
                            }
                        }
                        None => {
                            running = false;
                            break
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
#[derive(Parser, Debug)]
struct Args {
    /// Remote address to connect to
    remote_addr: Option<IpAddr>,
    /// Remote port to connect to
    remote_port: Option<u16>,
    /// Bind locally to this address
    #[arg(short, long)]
    local_addr: Option<IpAddr>,
    /// Bind locally to this port
    #[arg(short = 'p', long, default_value_t = 0)]
    local_port: u16,
    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
    /// Use UDP instead of TCP
    #[arg(short, long)]
    udp: bool,
    /// Use IPv6 instead of IPv4
    #[arg(short = '6', long)]
    ip_v6: bool,
    /// Quit when closing
    #[arg(short, long)]
    close_quit: bool,
    /// Reuse address
    #[arg(short, long)]
    reuse_addr: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let _remote_socket = match (args.remote_addr, args.remote_port) {
        (Some(addr), Some(port)) => match (addr, args.ip_v6) {
            (IpAddr::V6(addr), true) => Some(SocketAddr::V6(SocketAddrV6::new(addr, port, 0, 0))),
            (IpAddr::V4(addr), false) => Some(SocketAddr::V4(SocketAddrV4::new(addr, port))),
            (_, true) => {
                eprintln!("Remote address is not an IPv6 address");
                return ExitCode::FAILURE;
            }
            (_, false) => {
                eprintln!("Remote address is not an IPv4 address");
                return ExitCode::FAILURE;
            }
        },
        (None, Some(_)) | (Some(_), None) => {
            eprintln!("Both remote address and port required");
            return ExitCode::FAILURE;
        }
        (None, None) => None,
    };

    let local_port = args.local_port;
    let local_socket = match (args.local_addr, args.ip_v6) {
        (Some(IpAddr::V6(addr)), true) => SocketAddr::V6(SocketAddrV6::new(addr, local_port, 0, 0)),
        (Some(IpAddr::V4(addr)), false) => SocketAddr::V4(SocketAddrV4::new(addr, local_port)),
        (None, true) => SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, local_port, 0, 0)),
        (None, false) => SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, local_port)),
        (_, true) => {
            eprintln!("Local address is not an IPv6 address");
            return ExitCode::FAILURE;
        }
        (_, false) => {
            eprintln!("Local address is not an IPv4 address");
            return ExitCode::FAILURE;
        }
    };
    let join: JoinHandle<DynResult<()>>;
    let (send_msg, mut recv_msg) = mpsc::channel(10);
    let (send_cmd, mut recv_cmd) = mpsc::channel(10);
    join = tokio::spawn(tcp_server(local_socket, recv_cmd, send_msg));
    let (mut rl, mut out) = match Readline::new(">".to_string()) {
        Ok(res) => res,
        Err(e) => {
            eprintln!("Readline::new failed: {}", e);
            return ExitCode::FAILURE;
        }
    };
    rl.set_max_history(50);
    tokio::pin!(join);
    loop {
        tokio::select! {
            ret = rl.readline() => {
                match ret {
                    Ok(line) => {
                        let line = line.trim();
                        rl.add_history_entry(line.to_string());
                        let res = parser::byte_values(&line);
                        match res {
                            Ok((_left, b)) => {
                                dump_bytes(&mut out, "->", &b);
                                let _ = send_cmd.send(NetCmd::Send(b)).await;
                            }
                            Err(e) => {
                                writeln!(out, "Parse error: {e}").unwrap();
                            }
                        }
                    }
                    Err(e) => match e {
                        ReadlineError::Eof => {
                            break;
                        }
                        e => {
                            eprintln!("Readline::readline failed: {}", e);
                            return ExitCode::FAILURE;
                        }
                    },
                }
            }
            ret = &mut join => {
                match ret {
                    Err(e) => eprintln!("\nNetwork task faile"),
                    Ok(res) => {
                        match res {
                            Ok(_) =>
                                eprintln!("\nNetwork task exited"),
                            Err(e) => eprintln!("\n{}", e),
                        }
                    }
                }
                break;
            }
            res = recv_msg.recv() => {
                match res {
                    Some(msg) => {
                        match msg {
                            NetMessage::NewConnection(socket) =>
                                writeln!(out, "New connection from {}", socket).unwrap(),
                            NetMessage::Warning(w) =>
                                writeln!(out, "Warning: {}", w).unwrap(),
                            NetMessage::Disconnected(socket) => {
                                writeln!(out, "Disconnected from {}", socket).unwrap();
                            }
                            NetMessage::Data(bytes) => {
                                dump_bytes(&mut out, "<-", &bytes);
                            }
                        }
                    }
                    None => {
                        break;
                    }
                }

            }
        }
    }
    writeln!(out, "").unwrap();
    ExitCode::SUCCESS
}
