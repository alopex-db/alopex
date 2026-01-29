use std::collections::HashSet;

/// Test lanes for stress suite filtering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Lane {
    Smoke,
    Ci,
    Nightly,
    Weekly,
    Soak,
    Perf,
    Fuzz,
    Sanitizer,
}

impl Lane {
    pub const ALL: [Lane; 8] = [
        Lane::Smoke,
        Lane::Ci,
        Lane::Nightly,
        Lane::Weekly,
        Lane::Soak,
        Lane::Perf,
        Lane::Fuzz,
        Lane::Sanitizer,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Lane::Smoke => "smoke",
            Lane::Ci => "ci",
            Lane::Nightly => "nightly",
            Lane::Weekly => "weekly",
            Lane::Soak => "soak",
            Lane::Perf => "perf",
            Lane::Fuzz => "fuzz",
            Lane::Sanitizer => "sanitizer",
        }
    }
}

impl std::fmt::Display for Lane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn feature_enabled(lane: Lane) -> bool {
    match lane {
        Lane::Smoke => cfg!(feature = "lane_smoke"),
        Lane::Ci => cfg!(feature = "lane_ci"),
        Lane::Nightly => cfg!(feature = "lane_nightly"),
        Lane::Weekly => cfg!(feature = "lane_weekly"),
        Lane::Soak => cfg!(feature = "lane_soak"),
        Lane::Perf => cfg!(feature = "lane_perf"),
        Lane::Fuzz => cfg!(feature = "lane_fuzz"),
        Lane::Sanitizer => cfg!(feature = "lane_sanitizer"),
    }
}

fn any_feature_enabled() -> bool {
    Lane::ALL.iter().any(|lane| feature_enabled(*lane))
}

fn parse_lane_env(value: &str) -> HashSet<String> {
    let mut lanes = HashSet::new();
    let value = value.trim();
    if value.eq_ignore_ascii_case("all") || value == "*" {
        for lane in Lane::ALL {
            lanes.insert(lane.as_str().to_string());
        }
        return lanes;
    }
    for raw in value.split([',', ' ', ';']) {
        let lane = raw.trim();
        if lane.is_empty() {
            continue;
        }
        lanes.insert(lane.to_lowercase());
    }
    lanes
}

pub fn enabled_lanes() -> HashSet<String> {
    if let Ok(value) = std::env::var("STRESS_LANE") {
        if value.trim().is_empty() {
            return HashSet::new();
        }
        return parse_lane_env(&value);
    }

    if any_feature_enabled() {
        let mut lanes = HashSet::new();
        for lane in Lane::ALL {
            if feature_enabled(lane) {
                lanes.insert(lane.as_str().to_string());
            }
        }
        return lanes;
    }

    let mut lanes = HashSet::new();
    lanes.insert(Lane::Ci.as_str().to_string());
    lanes
}

pub fn should_run(lane: Lane) -> bool {
    let enabled = enabled_lanes();
    enabled.contains(lane.as_str())
}
