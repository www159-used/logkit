//! 内置 Body preset：[`body_preset!`](logen_model::body_preset) 构造 [`BodyConfig`]。

use logen_model::{body_preset, BodyConfig};

/// 紧凑单行 JSON 业务日志 body（原 `etc/body/json.yaml`）。
pub fn preset_json() -> BodyConfig {
    body_preset!(
        r#"{"event_time":"{}","level":"{}","service":"{}","node":"{}","tenant":"{}","trace_id":"{}","span_id":"{}","request_id":"{}","session_id":"{}","user_name":"{}","operator":"{}","client_ip":"{}","http_method":"{}","request_path":"{}","referer":"{}","user_agent":"{}","status_code":{},"ww_id":{},"latency_ms":{},"edge_pop":"{}","event_name":"{}","message":"{}"}"#,
        timestamp("%Y-%m-%dT%H:%M:%S%.3f%:z"),
        one_of { 3 => "info", 1 => "warn", 1 => "error" },
        one_of ["edge-gateway", "access-api", "logriver", "worker"],
        hostname,
        company_name,
        uuid_v4,
        uuid_v4,
        uuid_v4,
        uuid_v4,
        username,
        name_en,
        ipv4,
        one_of { 3 => "GET", 1 => "POST", 1 => "PUT" },
        url_path,
        url,
        user_agent,
        one_of { 3 => "200", 1 => "201", 1 => "400", 1 => "404", 1 => "500" },
        counter,
        integer(2, 900),
        template("{}-{}", lorem_word, domain_suffix),
        template("{}-{}", lorem_word, lorem_word),
        template("{}-{}", lorem_word, lorem_word),
    )
}

/// CEF 单行（原 `etc/body/cef.yaml`）。
pub fn preset_cef() -> BodyConfig {
    body_preset!(
        r#"CEF:0|Yottabyte|logen|1.0|{}|{}|{}|rt={} src={} dst={} spt={} dpt={} proto={} act={} outcome={} duser={} externalId={} cs1Label=tenant cs1={} cs2Label=trace_id cs2={} msg={}"#,
        template("{}-{}", lorem_word, lorem_word),
        template("{}-{}", lorem_word, lorem_word),
        integer(0, 10),
        timestamp("%Y-%m-%dT%H:%M:%S%.3f%:z"),
        ipv4,
        ipv4,
        integer(1024, 65535),
        one_of ["22", "53", "80", "443", "8443", "9200"],
        one_of { 3 => "TCP", 1 => "UDP", 1 => "ICMP" },
        one_of { 3 => "allow", 2 => "deny", 1 => "reset" },
        one_of { 3 => "success", 1 => "failure" },
        username,
        counter,
        company_name,
        uuid_v4,
        sentence(3, 10),
    )
}

/// LEEF v2 单行（原 `etc/body/leefv2.yaml`；扩展属性为 tab 分隔）。
pub fn preset_leefv2() -> BodyConfig {
    body_preset!(
        "LEEF:2.0|Yottabyte|logen|1.0|{}|devTime={}\tdevTimeFormat=yyyy-MM-dd'T'HH:mm:ss.SSSXXX\tsev={}\tcat={}\tsrc={}\tdst={}\tproto={}\tsrcPort={}\tdstPort={}\tusrName={}\taction={}\toutcome={}\tww_id={}\tmsg={}",
        template("{}-{}", lorem_word, lorem_word),
        timestamp("%Y-%m-%dT%H:%M:%S%.3f%:z"),
        integer(1, 10),
        one_of ["authentication", "firewall", "vpn", "endpoint", "audit"],
        ipv4,
        ipv4,
        one_of { 3 => "TCP", 1 => "UDP", 1 => "ICMP" },
        integer(1024, 65535),
        one_of ["22", "53", "80", "443", "8443", "9200"],
        username,
        one_of { 3 => "allow", 2 => "deny", 1 => "reset" },
        one_of { 3 => "success", 1 => "failure" },
        counter,
        sentence(3, 10),
    )
}

/// CyberArk syslog + CEF（原 `etc/body/cyberark.yaml`）。
pub fn preset_cyberark() -> BodyConfig {
    body_preset!(
        r#"<{}>0 {} {} CEF:{}|{}|{}|{}|{}|{}|{}|act={} suser={} fname={} dvc={} shost={} dhost={} duser={} externalId={} app={} reason={} cs1Label={} cs1={} cs2Label={} cs2={} msg={}"#,
        integer(1, 191),
        timestamp("%Y-%m-%dT%H:%M:%SZ"),
        hostname,
        one_of ["0", "1"],
        company_name,
        lorem_word,
        one_of ["1.0", "2.0"],
        integer(1, 99),
        lorem_word,
        integer(0, 10),
        lorem_word,
        username,
        url_path,
        ipv4,
        hostname,
        hostname,
        username,
        uuid_v4,
        one_of ["CyberArk", "PVWA", "CPM"],
        sentence(2, 8),
        one_of ["SessionID", "Safe"],
        uuid_v4,
        one_of ["Target", "Policy"],
        lorem_word,
        sentence(3, 12),
    )
}

/// Firewall_Winicssec（原 `etc/body/firewall-winicssec.yaml`）。
pub fn preset_firewall_winicssec() -> BodyConfig {
    body_preset!(
        r#"<{}>{} {} {}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^{}|^"#,
        integer(1, 191),
        timestamp("%Y-%m-%d %H:%M:%S"),
        hostname,
        one_of ["1", "2", "3"],
        uuid_v4,
        lorem_word,
        one_of ["1", "2", "3", "4", "5"],
        one_of ["0", "1"],
        one_of { 3 => "success", 1 => "failure" },
        one_of {
            1 => "-",
            1 => template("{}", sentence(2, 6)),
        },
        timestamp("%Y-%m-%d %H:%M:%S"),
        timestamp("%Y-%m-%d %H:%M:%S"),
        timestamp("%Y-%m-%d %H:%M:%S"),
        hostname,
        ipv4,
        ipv4,
        hostname,
        integer(1, 65535),
        ipv4,
        hostname,
        integer(1, 65535),
        one_of { 3 => "TCP", 1 => "UDP", 1 => "ICMP" },
        sentence(3, 10),
        one_of ["0", "1"],
        username,
        template(
            "{}:{}:{}:{}:{}:{}",
            integer(0, 255),
            integer(0, 255),
            integer(0, 255),
            integer(0, 255),
            integer(0, 255),
            integer(0, 255)
        ),
        template(
            "{}:{}:{}:{}:{}:{}",
            integer(0, 255),
            integer(0, 255),
            integer(0, 255),
            integer(0, 255),
            integer(0, 255),
            integer(0, 255)
        ),
        uuid_v4,
    )
}

/// IPS_Nsfocus（原 `etc/body/ips-nsfocus.yaml`）。
pub fn preset_ips_nsfocus() -> BodyConfig {
    body_preset!(
        r#"nsfocus:2 {} ips:{};danger_degree:{};breaking_sighn:{};event:[{}]{};src_addr:{};src_port:{};dst_addr:{};dst_port: {};user:admin;proto:{}"#,
        hostname,
        timestamp("%Y/%m/%d %H:%M:%S"),
        integer(1, 5),
        one_of ["0", "1"],
        template("{}_{}", integer(10000, 99999), integer(1, 999)),
        template(
            "{}({})",
            lorem_word,
            template("CVE-{}-{}", integer(2018, 2026), integer(1000, 99999))
        ),
        ipv4,
        integer(1024, 65535),
        ipv4,
        one_of ["80", "443", "8080", "8443"],
        one_of { 3 => "tcp", 1 => "udp" },
    )
}

/// Exchange_Tracking CSV（原 `etc/body/exchange-tracking.yaml`）。
pub fn preset_exchange_tracking() -> BodyConfig {
    body_preset!(
        r#"{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}"#,
        timestamp("%Y-%m-%d %H:%M:%S"),
        ipv4,
        hostname,
        ipv4,
        hostname,
        one_of ["SMTP", "MAPI", "Agent"],
        uuid_v4,
        one_of ["SMTP", "StoreDriver", "Agent"],
        uuid_v4,
        uuid_v4,
        uuid_v4,
        template("{}@{}", username, hostname),
        one_of { 3 => "Delivered", 1 => "Failed", 1 => "Pending" },
        integer(100, 5000000),
        integer(1, 50),
        one_of {
            1 => "",
            1 => template("{}@{}", username, hostname),
        },
        one_of {
            1 => "",
            1 => template("<{}@{}>", uuid_v4, hostname),
        },
        sentence(2, 12),
        template("{}@{}", username, hostname),
        one_of {
            1 => "",
            1 => template("{}@{}", username, hostname),
        },
        one_of {
            1 => "",
            1 => template("{};{}", integer(200, 599), lorem_word),
        },
        one_of ["Incoming", "Originating"],
        uuid_v4,
        ipv4,
        ipv4,
        one_of {
            1 => "",
            1 => template("ClientHostname={};ServerHostname={}", hostname, hostname),
        },
    )
}

/// Apache Combined + XFF（原 `etc/body/apache/access-xff.yaml`）。
pub fn preset_apache_access_xff() -> BodyConfig {
    body_preset!(
        r#"{} - {} [{}] "{} {} HTTP/1.1" {} {} "{}" "{}" "{}""#,
        ipv4,
        one_of {
            2 => "-",
            1 => template("{}", username),
        },
        timestamp("%d/%b/%Y:%H:%M:%S %z"),
        one_of { 3 => "GET", 1 => "POST", 1 => "HEAD" },
        one_of {
            1 => template(
                "/fig?type={}",
                one_of ["black", "white", "gray"]
            ),
            1 => template("{}", url_path),
        },
        one_of { 3 => "200", 1 => "404", 1 => "301", 1 => "500" },
        counter,
        one_of {
            3 => "-",
            1 => template("https://{}{}", hostname, url_path),
        },
        user_agent,
        one_of {
            2 => "-",
            1 => template("{}", ipv4),
            1 => template("{}, {}", ipv4, ipv4),
        },
    )
}

/// Middleware_Apache / ApacheAccess1（原 `etc/body/apache/middleware.yaml`）。
pub fn preset_apache_middleware() -> BodyConfig {
    body_preset!(
        r#"{} {} {} [{}] {} {} {} {} {}"#,
        ipv4,
        one_of {
            1 => "-",
            1 => template("{}", username),
        },
        one_of {
            2 => "-",
            1 => template("{}", username),
        },
        timestamp("%d/%b/%Y:%H:%M:%S %z"),
        template(
            r#""{} {} HTTP/{}""#,
            one_of { 3 => "GET", 1 => "POST", 1 => "HEAD" },
            one_of {
                1 => template(
                    "/fig?type={}",
                    one_of ["black", "white", "gray"]
                ),
                1 => template("{}", url_path),
            },
            one_of { 1 => "1.0", 2 => "1.1", 1 => "2.0" }
        ),
        one_of { 3 => "200", 1 => "404", 1 => "301", 1 => "500" },
        one_of {
            1 => "-",
            1 => template("{}", integer(0, 999999)),
        },
        one_of {
            2 => r#""-""#,
            1 => template(
                r#""{}""#,
                one_of {
                    1 => "-",
                    1 => template("https://{}{}", hostname, url_path),
                }
            ),
        },
        one_of {
            1 => r#""-""#,
            1 => template(r#""{}""#, user_agent),
        },
    )
}

/// 全部内置 preset 名（与脚本 builtin 一致）。
pub fn preset_names() -> &'static [&'static str] {
    &[
        "preset_json",
        "preset_cef",
        "preset_leefv2",
        "preset_cyberark",
        "preset_firewall_winicssec",
        "preset_ips_nsfocus",
        "preset_exchange_tracking",
        "preset_apache_access_xff",
        "preset_apache_middleware",
    ]
}

/// 按脚本函数名取 Body；未知名返回 `None`。
pub fn preset_by_name(name: &str) -> Option<BodyConfig> {
    Some(match name {
        "preset_json" => preset_json(),
        "preset_cef" => preset_cef(),
        "preset_leefv2" => preset_leefv2(),
        "preset_cyberark" => preset_cyberark(),
        "preset_firewall_winicssec" => preset_firewall_winicssec(),
        "preset_ips_nsfocus" => preset_ips_nsfocus(),
        "preset_exchange_tracking" => preset_exchange_tracking(),
        "preset_apache_access_xff" => preset_apache_access_xff(),
        "preset_apache_middleware" => preset_apache_middleware(),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logen_model::TemplateRunner;

    /// 测试内容：每个内置 preset 可编译为 runner 并产出非空行。
    /// 输入：`preset_names()` 全表。
    /// 预期：各 body fields 非空；`next_line` 非空。
    #[test]
    fn all_presets_render_nonempty() {
        for name in preset_names() {
            let body = preset_by_name(name).expect(name);
            assert!(!body.template.is_empty(), "{name}");
            assert!(!body.fields.is_empty(), "{name}");
            let mut r = TemplateRunner::try_new(body.template, body.fields).unwrap();
            assert!(!r.next_line().unwrap().is_empty(), "{name}");
        }
    }

    /// 测试内容：`body_preset!` 将空 `{}` 写成 `{{_bpN}}` 且与 field 一一对应。
    /// 输入：两槽模板 + timestamp/counter。
    /// 预期：键为 `_bp0`/`_bp1`；渲染行含 `|` 且 counter 为 0。
    #[test]
    fn body_preset_slots_map_to_generated_ids() {
        let body = body_preset!(r#"{}|{}"#, timestamp("%Y"), counter);
        assert_eq!(body.template, "{{_bp0}}|{{_bp1}}");
        assert!(body.fields.contains_key("_bp0"));
        assert!(body.fields.contains_key("_bp1"));
        let mut r = TemplateRunner::try_new(body.template, body.fields).unwrap();
        let line = r.next_line().unwrap();
        assert!(line.contains('|'), "{line}");
        assert!(line.ends_with("|0"), "{line}");
    }
}
