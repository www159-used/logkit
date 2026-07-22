pub fn read(key: &str) -> Option<String> {
    #[cfg(feature = "hydrate")]
    {
        web_sys::window()
            .and_then(|w| w.local_storage().ok().flatten())
            .and_then(|s| s.get_item(key).ok().flatten())
    }
    #[cfg(not(feature = "hydrate"))]
    {
        let _ = key;
        None
    }
}

pub fn write(key: &str, value: &str) {
    #[cfg(feature = "hydrate")]
    {
        if let Some(storage) = web_sys::window()
            .and_then(|w| w.local_storage().ok().flatten())
        {
            let _ = storage.set_item(key, value);
        }
    }
    #[cfg(not(feature = "hydrate"))]
    {
        let _ = (key, value);
    }
}
