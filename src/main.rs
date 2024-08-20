use tokio::fs;

use argmap;

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

use bollard::container::{AttachContainerOptions, CreateContainerOptions, AttachContainerResults};
use tokio::io::AsyncWriteExt;

const IMAGE: &str = "timescale/live-migration:latest";
const SNAPSHOT: &str = "live-migration-snapshot";
const MIGRATE: &str = "live-migration-migrate";
const CLEAN: &str = "live-migration-migrate";

struct LiveMigration {
    docker: Docker,
    source: String,
    target: String,
    dir: String,
}

impl LiveMigration {
    async fn new(
        docker: Docker,
        source: String,
        target: String,
        dir: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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
        Ok(Self {
            docker,
            source,
            target,
            dir,
        })
    }

    async fn create_container(
        &self,
        name: &str,
        args: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Add the following
        // - user (--user=$(id -u):$(id -g))
        // - pid (--pid=host)
        // - restart policy (--restart=always)
        let config = Config {
            image: Some(IMAGE.to_string()),
            cmd: Some(args),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            attach_stdin: Some(true),
            tty: Some(true),
            open_stdin: Some(true),
            env: Some(vec![
                format!("PGCOPYDB_TARGET_PGURI={}", self.target.clone(),),
                format!("PGCOPYDB_SOURCE_PGURI={}", self.source.clone(),),
            ]),
            host_config: Some(HostConfig {
                binds: Some(vec![format!("{}:/opt/timescale/ts_cdc", self.dir)]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name,
            ..Default::default()
        };

        let id = self
            .docker
            .create_container::<&str, String>(Some(options), config)
            .await?
            .id;

        Ok(self.docker.start_container::<String>(&id, None).await?)
    }

    async fn attach_container(&self, name: &str) -> Result<AttachContainerResults, Box<dyn std::error::Error>> {
            let options = Some(AttachContainerOptions::<&str> {
                stdin: Some(true),
                stdout: Some(true),
                stderr: Some(true),
                stream: Some(true),
                logs: Some(true),
                detach_keys: Some("ctrl-c"),
            });

            Ok(self.docker.attach_container(name, options).await?)
    }

    async fn snapshot(&self) -> Result<(), Box<dyn std::error::Error>> {
        let response = self.docker.inspect_container(SNAPSHOT, None).await;
        let create_snapshot = match response {
            Ok(r) => {
                // If the container is not running, remove it
                if r.state.as_ref().unwrap().running.unwrap() {
                    false
                } else {
                    self.docker.remove_container(SNAPSHOT, None).await?;
                    true
                }
            }
            Err(_) => true,
        };

        if create_snapshot {
            println!("Creating snapshot container");

            let args = vec!["snapshot".to_string()];

            self.create_container(SNAPSHOT, args).await?;

            let mut result = self.attach_container(SNAPSHOT).await?;

            while let Some(Ok(msg)) = result.output.next().await {
                print!("{msg}");
                if fs::read(format!("{}/snapshot", self.dir)).await.is_ok() {
                    println!("Snapshot file created");
                    break;
                }
            }
        } else {
            println!("Snapshot container already exists");
        }
        Ok(())
    }

    async fn migrate(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut args = vec!["migrate".to_string()];

        let migrate_running = match self.docker.inspect_container(MIGRATE, None).await {
            Ok(r) => {
                // If the container is not running, remove it
                if r.state.as_ref().unwrap().running.unwrap() {
                    true
                } else {
                    self.docker.remove_container(MIGRATE, None).await?;
                    args.push("--resume".to_string());
                    false
                }
            }
            Err(_) => false,
        };

        if !migrate_running {
            println!("Creating migrate container");
            self.create_container(MIGRATE, args).await?;
        }

        let mut result = self.attach_container(MIGRATE).await?;
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

    async fn clean(&self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (args, argv) = argmap::parse(std::env::args());

    // TODO: Use command line arguments instead of environment variables
    let source = env::var("PGCOPYDB_SOURCE_PGURI").expect("env PGCOPYDB_SOURCE_PGURI not set");
    let target = env::var("PGCOPYDB_TARGET_PGURI").expect("env PGCOPYDB_TARGET_PGURI not set");
    let dir = env::var("PGCOPYDB_DIR").expect("env PGCOPYDB_DIR not set");

    let _ = fs::create_dir(&dir).await;

    let docker = Docker::connect_with_socket_defaults().unwrap();
    let live_migration =
        LiveMigration::new(docker, source.clone(), target.clone(), dir.clone()).await?;

    if argv.contains_key("clean") {
        live_migration.clean().await?;
    } else {
        live_migration.snapshot().await?;
        live_migration.migrate().await?;
    }

    Ok(())
}
