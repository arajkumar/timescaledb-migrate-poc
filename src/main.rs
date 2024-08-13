use tokio::fs;

use bollard::container::Config;
use bollard::Docker;

use bollard::image::CreateImageOptions;
use bollard::models::HostConfig;
use futures_util::stream::StreamExt;
use futures_util::TryStreamExt;

use std::env;

use tokio::task::spawn;
use tokio::time::sleep;
use tokio::time::Duration;

use std::io::Read;

use termion::{async_stdin, terminal_size};

use bollard::container::{AttachContainerOptions, CreateContainerOptions};
use tokio::io::AsyncWriteExt;

const IMAGE: &str = "timescale/live-migration:latest";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = env::var("PGCOPYDB_SOURCE_PGURI")?;
    let target = env::var("PGCOPYDB_TARGET_PGURI")?;
    let dir = env::var("PGCOPYDB_DIR")?;

    fs::create_dir(&dir).await?;

    let docker = Docker::connect_with_socket_defaults().unwrap();

    docker
        .create_image(
            Some(CreateImageOptions {
                from_image: IMAGE,
                ..Default::default()
            }),
            None,
            None,
        )
        .try_collect::<Vec<_>>()
        .await?;

    let snapshot = "live-migration-snapshot";

    let response = docker.inspect_container(snapshot, None).await;

    let create_snapshot = match response {
        Ok(r) => {
            // If the container is not running, remove it
            if r.state.as_ref().unwrap().running.unwrap() {
                false
            } else {
                docker.remove_container(snapshot, None).await?;
                true
            }
        }
        Err(_) => true,
    };

    if create_snapshot {
        println!("Creating snapshot container");

        let args = vec!["snapshot".to_string()];
        let env = vec![
            format!("PGCOPYDB_TARGET_PGURI={}", target.clone(),),
            format!("PGCOPYDB_SOURCE_PGURI={}", source.clone(),),
        ];

        let alpine_config = Config {
            image: Some(IMAGE.to_string()),
            cmd: Some(args),
            tty: Some(true),
            env: Some(env),
            host_config: Some(HostConfig {
                binds: Some(vec![format!("{dir}:/opt/timescale/ts_cdc")]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: snapshot,
            ..Default::default()
        };

        let id = docker
            .create_container::<&str, String>(Some(options), alpine_config)
            .await?
            .id;
        docker.start_container::<String>(&id, None).await?;

        let options = Some(AttachContainerOptions::<&str> {
            stdin: Some(true),
            stdout: Some(true),
            stderr: Some(true),
            stream: Some(true),
            logs: Some(true),
            detach_keys: Some("ctrl-c"),
        });

        let mut result = docker.attach_container(&id, options).await?;
        while let Some(Ok(msg)) = result.output.next().await {
            print!("{msg}");
            if let Ok(_) = fs::read(format!("{dir}/snapshot")).await {
                println!("Snapshot file created");
                break;
            }
        }
    } else {
        println!("Snapshot container already exists");
    }

    // Check dir has a snapshot file created with content.
    // Now run migrate
    let migrate = "live-migration-migrate";

    let mut args = vec!["migrate".to_string()];

    let migrate_running = match docker.inspect_container(migrate, None).await {
        Ok(r) => {
            // If the container is not running, remove it
            if r.state.as_ref().unwrap().running.unwrap() {
                true
            } else {
                docker.remove_container(migrate, None).await?;
                args.push("--resume".to_string());
                false
            }
        }
        Err(_) => false,
    };

    if !migrate_running {
        println!("Creating migrate container");

        let alpine_config = Config {
            image: Some(IMAGE.to_string()),
            cmd: Some(args),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            attach_stdin: Some(true),
            tty: Some(true),
            open_stdin: Some(true),
            env: Some(vec![
                format!("PGCOPYDB_TARGET_PGURI={}", target.clone(),),
                format!("PGCOPYDB_SOURCE_PGURI={}", source.clone(),),
            ]),
            host_config: Some(HostConfig {
                binds: Some(vec![format!("{dir}:/opt/timescale/ts_cdc")]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: migrate,
            ..Default::default()
        };

        let id = docker
            .create_container::<&str, String>(Some(options), alpine_config)
            .await?
            .id;
        docker.start_container::<String>(&id, None).await?;
    }

    let options = Some(AttachContainerOptions::<&str> {
        stdin: Some(true),
        stdout: Some(true),
        stderr: Some(true),
        stream: Some(true),
        logs: Some(true),
        detach_keys: Some("ctrl-c"),
    });

    let mut result = docker.attach_container(migrate, options).await?;
    // pipe stdin into the docker exec stream input
    spawn(async move {
        let mut stdin = async_stdin().bytes();
        loop {
            if let Some(Ok(byte)) = stdin.next() {
                result.input.write_all(&[byte]).await.ok();
            } else {
                sleep(Duration::from_nanos(10)).await;
            }
        }
    });

    while let Some(Ok(msg)) = result.output.next().await {
        print!("{msg}");
    }
    Ok(())
}
