use crate::theme::{ThemePreference, use_theme};

use leptos::prelude::*;
use plotters::prelude::*;

const MAX_SAMPLES: usize = 60;
const CHART_WIDTH: u32 = 640;
const CHART_HEIGHT: u32 = 160;

struct ChartPalette {
    grid: RGBColor,
    line: RGBColor,
}

impl ChartPalette {
    fn for_theme(dark: bool) -> Self {
        if dark {
            Self {
                grid: RGBColor(120, 120, 120),
                line: RGBColor(0, 122, 204),
            }
        } else {
            Self {
                grid: RGBColor(160, 160, 170),
                line: RGBColor(0, 122, 204),
            }
        }
    }
}

#[component]
pub fn EpsChart(samples: ReadSignal<Vec<f64>>) -> impl IntoView {
    let theme = use_theme();

    view! {
        <div class="eps-chart-wrap card">
            {move || {
                let values = samples.get();
                if values.is_empty() {
                    return view! { <p class="muted chart-empty">"—"</p> }.into_any();
                }
                let latest = *values.last().unwrap_or(&0.0);
                let dark = chart_prefers_dark(theme.preference.get());
                let svg = render_eps_chart(&values, dark).unwrap_or_default();
                view! {
                    <div class="chart-head">
                        <span class="chart-latest mono">{format!("{latest:.1} EPS")}</span>
                    </div>
                    <div class="eps-chart" inner_html=svg></div>
                }.into_any()
            }}
        </div>
    }
}

fn chart_prefers_dark(preference: ThemePreference) -> bool {
    match preference {
        ThemePreference::Dark => true,
        ThemePreference::Light => false,
        ThemePreference::System => {
            #[cfg(feature = "hydrate")]
            {
                web_sys::window()
                    .and_then(|w| w.match_media("(prefers-color-scheme: dark)").ok())
                    .flatten()
                    .map(|mq| mq.matches())
                    .unwrap_or(false)
            }
            #[cfg(not(feature = "hydrate"))]
            {
                false
            }
        }
    }
}

fn render_eps_chart(samples: &[f64], dark: bool) -> Result<String, Box<dyn std::error::Error>> {
    let palette = ChartPalette::for_theme(dark);
    let mut buffer = String::new();
    {
        let root = SVGBackend::with_string(&mut buffer, (CHART_WIDTH, CHART_HEIGHT))
            .into_drawing_area();
        root.fill(&RGBAColor(0, 0, 0, 0.0))?;

        let x_max = (samples.len().saturating_sub(1).max(1)) as f64;
        let y_max = samples.iter().copied().fold(0.01f64, f64::max) * 1.08;

        let mut chart = ChartBuilder::on(&root)
            .margin(8)
            .x_label_area_size(0)
            .y_label_area_size(0)
            .build_cartesian_2d(0f64..x_max, 0f64..y_max)?;

        chart
            .configure_mesh()
            .light_line_style(palette.grid.mix(0.18))
            .bold_line_style(palette.grid.mix(0.38))
            .x_labels(0)
            .y_labels(0)
            .draw()?;

        chart.draw_series(LineSeries::new(
            samples.iter().enumerate().map(|(i, v)| (i as f64, *v)),
            palette.line.stroke_width(2),
        ))?;

        root.present()?;
    }
    Ok(buffer)
}

pub fn push_eps_sample(history: &mut Vec<f64>, eps: f64) {
    history.push(eps);
    if history.len() > MAX_SAMPLES {
        let drain = history.len() - MAX_SAMPLES;
        history.drain(0..drain);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试内容：plotters SVG 后端能为 EPS 样本序列生成非空 SVG。
    /// 输入：含 3 个浮点样本的切片，浅色主题。
    /// 预期：返回 Ok，且 SVG 字符串包含 `<svg`。
    #[test]
    fn render_eps_chart_outputs_svg() {
        let samples = [10.0, 20.0, 15.0];
        let svg = render_eps_chart(&samples, false).expect("chart");
        assert!(svg.contains("<svg"));
    }
}
