struct ServerTask(tauri::async_runtime::JoinHandle<()>);

fn install_app_menu(app: &tauri::App) -> tauri::Result<()> {
    use tauri::menu::{AboutMetadata, MenuBuilder, SubmenuBuilder};

    let about = AboutMetadata {
        name: Some("Logkit".into()),
        version: Some(env!("CARGO_PKG_VERSION").into()),
        copyright: Some("Copyright © Logkit contributors".into()),
        icon: app.default_window_icon().cloned(),
        ..Default::default()
    };

    let app_menu = SubmenuBuilder::new(app, "Logkit")
        .about(Some(about))
        .separator()
        .services()
        .separator()
        .hide()
        .hide_others()
        .show_all()
        .separator()
        .quit()
        .build()?;

    let edit_menu = SubmenuBuilder::new(app, "Edit")
        .undo()
        .redo()
        .separator()
        .cut()
        .copy()
        .paste()
        .select_all()
        .build()?;

    let menu = MenuBuilder::new(app)
        .items(&[&app_menu, &edit_menu])
        .build()?;

    app.set_menu(menu)?;
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_log::Builder::default().build())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_process::init())
        .setup(|app| {
            install_app_menu(app)?;

            #[cfg(not(debug_assertions))]
            {
                use leptos::prelude::get_configuration;
                use tauri::{Manager, Url};

                if std::env::var("LEPTOS_OUTPUT_NAME").is_err() {
                    std::env::set_var("LEPTOS_OUTPUT_NAME", "logkit-web");
                }

                let resource_dir = app.path().resource_dir().map_err(|e| {
                    Box::<dyn std::error::Error>::from(format!(
                        "Failed to get resource directory: {e}"
                    ))
                })?;
                let site_root = resource_dir.join("site");
                let cargo_toml_path = resource_dir.join("Cargo.toml");

                std::env::set_var(
                    "LEPTOS_SITE_ROOT",
                    site_root.to_string_lossy().to_string(),
                );

                let cargo_toml_str = cargo_toml_path.to_str().ok_or_else(|| {
                    Box::<dyn std::error::Error>::from("Cargo.toml path is not valid UTF-8")
                })?;
                let mut conf = get_configuration(Some(cargo_toml_str)).map_err(|e| {
                    Box::<dyn std::error::Error>::from(format!(
                        "Failed to load leptos configuration: {e}"
                    ))
                })?;
                conf.leptos_options.site_root = site_root.to_string_lossy().to_string().into();

                let router = logkit_web_app::build_router(conf.leptos_options);

                let (port, listener) = tauri::async_runtime::block_on(async {
                    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
                        Ok(l) => l,
                        Err(_) => tokio::net::TcpListener::bind("[::1]:0")
                            .await
                            .map_err(|e| {
                                Box::<dyn std::error::Error>::from(format!(
                                    "Failed to bind tcp listener: {e}"
                                ))
                            })?,
                    };
                    let port = listener.local_addr()?.port();
                    Ok::<_, Box<dyn std::error::Error>>((port, listener))
                })?;

                let server_task = tauri::async_runtime::spawn(async move {
                    let _ = axum::serve(listener, router.into_make_service()).await;
                });
                app.manage(ServerTask(server_task));

                tauri::async_runtime::block_on(async {
                    let addr = format!("127.0.0.1:{port}");
                    for _ in 0..50 {
                        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                });

                let window = app.get_webview_window("main").ok_or_else(|| {
                    Box::<dyn std::error::Error>::from("Failed to get main window")
                })?;
                let url = Url::parse(&format!("http://127.0.0.1:{port}")).map_err(|e| {
                    Box::<dyn std::error::Error>::from(format!("Failed to parse URL: {e}"))
                })?;
                window.navigate(url).map_err(|e| {
                    Box::<dyn std::error::Error>::from(format!("Failed to navigate window: {e}"))
                })?;
            }

            #[cfg(debug_assertions)]
            let _ = app;

            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { .. } = event {
                use tauri::Manager;
                if let Some(task) = window.try_state::<ServerTask>() {
                    task.0.abort();
                }
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
