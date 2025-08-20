use anyhow::Result;

mod auth;
mod com;
mod consts;
mod packets;
mod variable;
mod mysqld;


#[tokio::main]
async fn main() -> Result<()> {

    let log4rs_config_file = option_env!("MYSQL_AVATAR_LOG4RS_FILE").unwrap_or("./log4rs.toml");
    log4rs::init_file(log4rs_config_file, Default::default())?;

    if let Err(e) = mysqld::init_server(8080).await {
        log::error!("Startup server failed, {e}");
    }

    Ok(())
}
