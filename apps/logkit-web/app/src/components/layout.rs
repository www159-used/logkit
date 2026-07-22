use leptos::prelude::*;

#[component]
pub fn PageShell(children: Children) -> impl IntoView {
    view! {
        <div class="page">
            {children()}
        </div>
    }
}

#[component]
pub fn PageHeader(children: Children) -> impl IntoView {
    view! {
        <header class="header">
            {children()}
        </header>
    }
}

#[component]
pub fn PageHeaderMain(children: Children) -> impl IntoView {
    view! {
        <div>
            {children()}
        </div>
    }
}

#[component]
pub fn PageHeaderActions(children: Children) -> impl IntoView {
    view! {
        <div class="header-actions">
            {children()}
        </div>
    }
}

#[component]
pub fn Breadcrumb(children: Children) -> impl IntoView {
    view! {
        <p class="breadcrumb">
            {children()}
        </p>
    }
}

#[component]
pub fn PageTitle(children: Children) -> impl IntoView {
    view! {
        <h1 class="title">
            {children()}
        </h1>
    }
}

#[component]
pub fn PageSubtitle(
    #[prop(default = "")]
    class: &'static str,
    children: Children,
) -> impl IntoView {
    let class = if class.is_empty() {
        "subtitle".to_string()
    } else {
        format!("subtitle {class}")
    };
    view! {
        <p class=class>
            {children()}
        </p>
    }
}

#[component]
pub fn EmptyState(children: Children) -> impl IntoView {
    view! {
        <div class="empty">
            {children()}
        </div>
    }
}

#[component]
pub fn SectionHeading(children: Children) -> impl IntoView {
    view! {
        <h2 class="section-heading">
            {children()}
        </h2>
    }
}
