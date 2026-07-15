use std::collections::BTreeMap;

use logen_model::{FieldSpec, OneOfBranch, TemplateRunner};

#[test]
fn weighted_one_of_prefers_heavy_branch() {
    let fields = BTreeMap::from([(
        "x".to_string(),
        FieldSpec::OneOf {
            branches: vec![
                OneOfBranch::WeightedLiteral {
                    w: 1,
                    v: "rare".into(),
                },
                OneOfBranch::WeightedLiteral {
                    w: 9,
                    v: "often".into(),
                },
            ],
        },
    )]);
    let mut r = TemplateRunner::try_new("{{x}}", fields).unwrap();
    let mut often = 0usize;
    for _ in 0..2000 {
        if r.next_line().unwrap() == "often" {
            often += 1;
        }
    }
    assert!(often > 1500, "often={often}");
}

#[test]
fn one_of_template_branch_accepts_w() {
    let yaml = r#"
type: one-of
branches:
  - { w: 3, v: "-" }
  - w: 1
    template: "{{x}}"
    fields:
      x:
        type: counter
"#;
    let spec: FieldSpec = serde_yaml::from_str(yaml).unwrap();
    let fields = BTreeMap::from([("f".to_string(), spec)]);
    let mut r = TemplateRunner::try_new("{{f}}", fields).unwrap();
    let mut dash = 0usize;
    for _ in 0..400 {
        if r.next_line().unwrap() == "-" {
            dash += 1;
        }
    }
    assert!(dash > 250, "dash={dash}");
}

#[test]
fn pick_type_is_rejected_in_yaml() {
    let yaml = r#"
type: pick
values: [a, b]
"#;
    let err = serde_yaml::from_str::<FieldSpec>(yaml).unwrap_err();
    assert!(err.to_string().contains("pick"), "err={err}");
}
