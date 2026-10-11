use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Api {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Tapo {
    pub username: String,
    pub password: String,
    pub refresh_rate_s: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Mqtt {
    pub address: String,
    pub topic_name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Device {
    pub ip_address: String,
    pub name: String,
    pub record_time_usage: bool,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub api: Api,
    pub tapo: Tapo,
    pub mqtt: Mqtt,
    pub devices: Vec<Device>,
}

impl Settings {
    pub fn new() -> Result<Self, anyhow::Error> {
        let base_path = std::env::current_dir().expect("failed to determine the current directory");

        let mut builder = config::Config::builder();

        let config_path = base_path.join("settings.yaml");
        builder = builder.add_source(config::File::new(
            config_path.to_str().expect("this should never happen"),
            config::FileFormat::Yaml,
        ));

        let settings = builder.build()?.try_deserialize::<Self>()?;

        Ok(settings)
    }
}
