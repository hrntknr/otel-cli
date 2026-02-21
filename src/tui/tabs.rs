#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Logs,
    Traces,
    Metrics,
}

impl Tab {
    pub fn title(&self) -> &str {
        match self {
            Tab::Logs => "Logs",
            Tab::Traces => "Traces",
            Tab::Metrics => "Metrics",
        }
    }

    pub fn all() -> &'static [Tab] {
        &[Tab::Logs, Tab::Traces, Tab::Metrics]
    }

    pub fn next(&self) -> Tab {
        match self {
            Tab::Logs => Tab::Traces,
            Tab::Traces => Tab::Metrics,
            Tab::Metrics => Tab::Logs,
        }
    }

    pub fn prev(&self) -> Tab {
        match self {
            Tab::Logs => Tab::Metrics,
            Tab::Traces => Tab::Logs,
            Tab::Metrics => Tab::Traces,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tab_next() {
        assert_eq!(Tab::Logs.next(), Tab::Traces);
        assert_eq!(Tab::Traces.next(), Tab::Metrics);
        assert_eq!(Tab::Metrics.next(), Tab::Logs);
    }

    #[test]
    fn tab_prev() {
        assert_eq!(Tab::Logs.prev(), Tab::Metrics);
        assert_eq!(Tab::Traces.prev(), Tab::Logs);
        assert_eq!(Tab::Metrics.prev(), Tab::Traces);
    }

    #[test]
    fn tab_title() {
        assert_eq!(Tab::Traces.title(), "Traces");
        assert_eq!(Tab::Logs.title(), "Logs");
        assert_eq!(Tab::Metrics.title(), "Metrics");
    }

    #[test]
    fn tab_all() {
        let all = Tab::all();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0], Tab::Logs);
        assert_eq!(all[1], Tab::Traces);
        assert_eq!(all[2], Tab::Metrics);
    }
}
