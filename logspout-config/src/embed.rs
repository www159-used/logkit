use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "assets/"]
pub struct Assets;

pub fn ref_toml_string() -> Result<String, crate::LogspoutError> {
    let bytes = Assets::get("conf.ref.toml")
        .map(|a| a.data)
        .ok_or_else(|| crate::LogspoutError::EmbeddedMissing("conf.ref.toml".to_string()))?;
    let s = std::str::from_utf8(bytes.as_ref())?;
    Ok(s.to_owned())
}
