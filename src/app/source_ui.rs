//! Permission, polling mode and observed endpoint traffic are separate columns.
pub(super) mod zmq_setup;

use super::*;
use crate::sources::{
    ChainCheck, Communication, CommunicationOutcome, ConnectionState, Control, Registry,
};
use ratatui::widgets::{Cell, HighlightSpacing, Row, Table, TableState};

mod activity;
pub(super) use activity::Activity;

fn render_source_table(
    frame: &mut Frame<'_>,
    theme: &Theme,
    table: Table<'_>,
    area: Rect,
    selected: Option<usize>,
    header_height: u16,
    total: usize,
) {
    let rail = Rect::new(
        area.x,
        area.y.saturating_add(header_height),
        area.width,
        area.height.saturating_sub(header_height),
    );
    let visible = usize::from(rail.height);
    let overflowing = selected.is_some() && total > visible;
    let mut state = TableState::default().with_selected(selected);
    frame.render_stateful_widget(
        table
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol("  ")
            .row_highlight_style(
                Style::default()
                    .bg(theme.panel)
                    .add_modifier(Modifier::BOLD),
            ),
        scroll_view::content_area(area, overflowing),
        &mut state,
    );
    // Table applies the row highlight after the symbol, so draw the cursor
    // separately to keep its colors. Source tables have one line per row.
    if let Some(offset) = state
        .selected()
        .and_then(|index| index.checked_sub(state.offset()))
        && offset < visible
        && area.width != 0
    {
        render_selection_gutter(
            frame,
            theme,
            Rect::new(area.x, area.y + header_height + offset as u16, 1, 1),
        );
    }
    if overflowing {
        scroll_view::render(frame, theme, rail, total, state.offset(), visible);
    }
}

fn communication_label(
    state: ConnectionState,
    status: &Communication,
    stale_after: Duration,
) -> (&'static str, u8) {
    if state == ConnectionState::Disabled {
        ("—", 0)
    } else if !status.in_flight.is_empty() {
        ("Working", 1)
    } else {
        match status.outcome {
            Some(CommunicationOutcome::Failed) => ("Unavailable", 3),
            Some(CommunicationOutcome::Responding)
                if status
                    .last_contact
                    .is_some_and(|time| time.elapsed() <= stale_after) =>
            {
                ("Responding", 2)
            }
            Some(_) => ("Idle", 0),
            None => ("Not used", 0),
        }
    }
}

fn age_label(status: &Communication) -> String {
    status
        .last_success
        .map_or_else(|| "—".into(), |time| short_age(time.elapsed().as_secs()))
}

fn short_age(seconds: u64) -> String {
    match seconds {
        0 => "<1s".into(),
        1..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86400 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86400),
    }
}

fn chain_check_line(theme: &Theme, control: &Control, stale_after: Duration) -> Line<'static> {
    let (status, style) = match control.chain_check() {
        Some(ChainCheck::Checking) if control.polling() => ("Checking".into(), theme.link()),
        Some(ChainCheck::Finished { at, error }) => {
            let fresh = control.polling() && at.elapsed() <= stale_after;
            let age = short_age(at.elapsed().as_secs());
            match error {
                Some(error) => {
                    let label = if fresh {
                        "Needs attention"
                    } else {
                        "Last check incomplete"
                    };
                    let reason = error.lines().next().unwrap_or_default();
                    (
                        format!("{label} · {age} ago · {reason}"),
                        if fresh {
                            theme.warning()
                        } else {
                            theme.muted()
                        },
                    )
                }
                None if fresh => (format!("Reconciled · {age} ago"), theme.positive()),
                None => (format!("Last reconciled {age} ago"), theme.muted()),
            }
        }
        _ if control.polling() => ("Awaiting first check".into(), theme.muted()),
        _ => ("Not polling".into(), theme.muted()),
    };
    Line::from(vec![
        Span::styled("Chain check  ", theme.muted()),
        Span::styled(status, style),
    ])
}

fn communication_style(theme: &Theme, level: u8) -> Style {
    match level {
        1 => theme.link(),
        2 => theme.positive(),
        3 => theme.negative(),
        _ => theme.muted(),
    }
}

fn mode_description(state: ConnectionState) -> &'static str {
    match state {
        ConnectionState::Disabled => "Disabled — no requests permitted",
        ConnectionState::Enabled => "Enabled — active-chain polling and on-demand requests",
        ConnectionState::Monitored => {
            "Monitored — enabled, with background polling on other chains too"
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn render(
    frame: &mut Frame<'_>,
    theme: &Theme,
    registry: &Registry,
    names: &[String],
    current: usize,
    selected: usize,
    message: &str,
    activity: &mut Activity,
) {
    render_rows(
        frame,
        theme,
        &registry.configs,
        &registry.rows(),
        names,
        current,
        selected,
        message,
        activity,
    );
}

fn zmq_status_line(theme: &Theme, control: &crate::sources::Control) -> Line<'static> {
    use crate::zmq::FeedStatus;

    let feeds = control.zmq.feeds();
    let count = feeds.len();
    let connected = feeds
        .iter()
        .filter(|(_, _, status)| matches!(status, FeedStatus::Connected | FeedStatus::Receiving(_)))
        .count();
    let received = feeds
        .iter()
        .filter_map(|(_, _, status)| match status {
            FeedStatus::Receiving(last) => Some(*last),
            _ => None,
        })
        .collect::<Vec<_>>();
    let unavailable = feeds
        .iter()
        .filter(|(_, _, status)| {
            matches!(
                status,
                FeedStatus::Conflict | FeedStatus::Unreachable | FeedStatus::Error(_)
            )
        })
        .count();
    let config_error = !control.zmq.settings().errors.is_empty();
    let (mut summary, style) = if count == 0 && !config_error {
        ("Not configured".into(), theme.muted())
    } else if !control.polling() && count > 0 {
        let text = if control.state() == ConnectionState::Disabled {
            "Configured · source disabled"
        } else {
            "Configured · select or monitor this source"
        };
        (text.into(), theme.muted())
    } else if let Some(last) = received.iter().max() {
        (
            format!(
                "Receiving · last {} ago · {}/{} topics seen",
                short_age(last.elapsed().as_secs()),
                received.len(),
                count
            ),
            theme.positive(),
        )
    } else if connected > 0 {
        (
            format!("Connected · waiting for events · {connected}/{count} topics connected"),
            theme.link(),
        )
    } else if config_error
        || feeds
            .iter()
            .any(|(_, _, status)| matches!(status, FeedStatus::Error(_)))
    {
        ("Setup needs attention".into(), theme.warning())
    } else if unavailable > 0 {
        ("Unreachable · retrying".into(), theme.warning())
    } else {
        ("Waiting for connection".into(), theme.muted())
    };
    if connected > 0 && unavailable > 0 {
        summary.push_str(&format!(" · {unavailable} unavailable"));
    }
    Line::from(vec![
        Span::styled("ZMQ  ", theme.muted()),
        Span::styled(summary, style),
        Span::styled("  ·  z setup", theme.muted()),
    ])
}

#[allow(clippy::too_many_arguments)]
fn render_rows(
    frame: &mut Frame<'_>,
    theme: &Theme,
    configs: &[Config],
    sources: &[(usize, Option<usize>)],
    names: &[String],
    current: usize,
    selected: usize,
    message: &str,
    activity: &mut Activity,
) {
    let inner = render_workspace(
        frame,
        theme,
        "Sources — F2",
        "j/k select | Enter current chain | e enable/disable | m monitoring | s save | a activity | z ZMQ setup | Esc close | q quit",
    );
    let instructions = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("* current chain   ", theme.focused()),
            Span::styled("Enabled = permitted   ", theme.positive()),
            Span::styled("Monitor = enabled + background polling", theme.link()),
        ]),
        Line::from(Span::styled(
            "Communication: observed requests. Last OK: age of last successful reply.",
            theme.muted(),
        )),
    ])
    .style(theme.base())
    .wrap(Wrap { trim: true });
    let help_height = u16::try_from(instructions.line_count(inner.width.max(1))).unwrap_or(3);
    let footer_height = inner
        .height
        .saturating_sub(help_height.saturating_add(5))
        .min(13);
    let regions = Layout::vertical([
        Constraint::Length(help_height.saturating_add(1)),
        Constraint::Min(3),
        Constraint::Length(footer_height),
    ])
    .split(inner);
    frame.render_widget(instructions, regions[0]);

    let wide = regions[1].width >= 110;
    let show_age = regions[1].width >= 75;
    let (headers, widths) = if wide {
        (
            vec![
                "",
                "Chain",
                "Source",
                "Endpoint",
                "Enabled",
                "Monitor",
                "Connection",
                "Last OK",
            ],
            vec![
                Constraint::Length(1),
                Constraint::Fill(1),
                Constraint::Length(6),
                Constraint::Fill(2),
                Constraint::Length(8),
                Constraint::Length(8),
                Constraint::Length(13),
                Constraint::Length(8),
            ],
        )
    } else if show_age {
        (
            vec![
                "",
                "Chain",
                "Source",
                "Enabled",
                "Monitor",
                "Connection",
                "Last OK",
            ],
            vec![
                Constraint::Length(1),
                Constraint::Fill(1),
                Constraint::Length(6),
                Constraint::Length(7),
                Constraint::Length(7),
                Constraint::Length(12),
                Constraint::Length(7),
            ],
        )
    } else {
        (
            vec!["", "Chain", "Source", "On", "Monitor", "Communication"],
            vec![
                Constraint::Length(1),
                Constraint::Fill(1),
                Constraint::Length(6),
                Constraint::Length(6),
                Constraint::Length(6),
                Constraint::Length(11),
            ],
        )
    };
    let mut rows = Vec::new();
    let mut details = Vec::new();
    for (index, &(chain, api)) in sources.iter().enumerate() {
        let config = &configs[chain];
        let name = names.get(chain).unwrap_or(&config.chain.name);
        let (kind, endpoint, control) = if let Some(api) = api {
            let api = &config.daemon.api_pool.endpoints[api];
            ("API", api.url.clone(), &api.control)
        } else {
            (
                "Daemon",
                format!("http://{}:{}", config.daemon.host, config.daemon.port),
                &config.daemon.control,
            )
        };
        let state = control.state();
        let status = control.communication();
        let stale_after =
            Duration::from_secs(config.poll_interval_seconds.saturating_mul(2).max(30));
        let (communication, level) = communication_label(state, &status, stale_after);
        let enabled = state != ConnectionState::Disabled;
        let enabled_label = if enabled {
            format!("{} Yes", theme.glyph("●", "+"))
        } else {
            format!("{} No", theme.glyph("○", "-"))
        };
        let monitor_label = if state == ConnectionState::Monitored {
            format!("{} Yes", theme.glyph("↻", "~"))
        } else {
            "—".into()
        };
        let mut cells = vec![
            Cell::from(if chain == current { "*" } else { "" }).style(theme.focused()),
            Cell::from(name.clone()).style(theme.link()),
            Cell::from(kind),
            Cell::from(enabled_label).style(if enabled {
                theme.positive()
            } else {
                theme.muted()
            }),
            Cell::from(monitor_label).style(if state == ConnectionState::Monitored {
                theme.link()
            } else {
                theme.muted()
            }),
            Cell::from(communication).style(communication_style(theme, level)),
        ];
        if wide {
            cells.insert(3, Cell::from(endpoint.clone()));
        }
        if show_age {
            cells.push(
                Cell::from(Line::from(age_label(&status)).alignment(Alignment::Right))
                    .style(theme.muted()),
            );
        }
        rows.push(Row::new(cells));
        if index == selected {
            details = vec![
                Line::from(Span::styled(format!("{name} — {kind}"), theme.focused())),
                identity_property_line(theme, "Identity", config.chain_cache_key()),
                identity_property_line(theme, "Directory", chain_directory_label(config)),
                identity_property_line(theme, "Config file", config_file_label(config)),
                identity_property_line(theme, "Endpoint", endpoint),
                identity_property_line(
                    theme,
                    "Mode",
                    if api.is_some() && state == ConnectionState::Enabled {
                        "Enabled — available for API fallback requests".into()
                    } else {
                        mode_description(state).into()
                    },
                ),
                Line::from(vec![
                    Span::styled("Communication: ", theme.muted()),
                    Span::styled(communication, communication_style(theme, level)),
                    Span::raw(format!("   Last OK: {}", age_label(&status))),
                ]),
            ];
            if api.is_none() {
                details.push(identity_property_line(
                    theme,
                    "Poll interval",
                    format!("{}s", config.poll_interval_seconds),
                ));
            }
            if api.is_none() {
                details.insert(1, chain_check_line(theme, control, stale_after));
                if config.daemon.config_file.is_some()
                    && crate::zmq::setup::local_host(&config.daemon.host)
                {
                    details.insert(2, zmq_status_line(theme, control));
                }
            }
        }
    }
    let table = Table::new(rows, widths)
        .header(
            Row::new(headers)
                .style(theme.muted().add_modifier(Modifier::BOLD))
                .bottom_margin(1),
        )
        .column_spacing(1)
        .style(theme.base());
    render_source_table(
        frame,
        theme,
        table,
        regions[1],
        (!sources.is_empty()).then_some(selected),
        2,
        sources.len(),
    );
    if !message.is_empty() {
        details.push(Line::from(Span::styled(
            message.to_string(),
            theme.warning(),
        )));
    }
    let details_height = (details.len() as u16).min(regions[2].height.saturating_sub(3));
    let footer = Layout::vertical([Constraint::Length(details_height), Constraint::Min(0)])
        .split(regions[2]);
    frame.render_widget(Paragraph::new(details).style(theme.base()), footer[0]);
    if let Some(&(chain, api)) = sources.get(selected) {
        activity.preview(frame, footer[1], theme, &configs[chain], api);
        activity.render_history(frame, theme, &configs[chain], api, |system| {
            configs
                .iter()
                .position(|config| config.chain_cache_key() == system)
                .map(|index| {
                    names
                        .get(index)
                        .unwrap_or(&configs[index].chain.name)
                        .clone()
                })
                .unwrap_or_else(|| system.into())
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::{ApiEndpoint, ApiPool};

    fn column(row: &str, text: &str) -> Option<usize> {
        row.find(text).map(|index| row[..index].chars().count())
    }

    #[test]
    fn source_table_aligns_rows_and_keeps_monitored_sources_enabled() {
        let mut configs = Vec::new();
        for (index, state) in [
            ConnectionState::Enabled,
            ConnectionState::Monitored,
            ConnectionState::Disabled,
        ]
        .into_iter()
        .enumerate()
        {
            let mut config = super::super::tests::regression_config();
            config.chain.name = format!("chain{index}");
            config.daemon.port = 21000 + index as u16;
            config.daemon.control = Arc::new(Control::default());
            config.daemon.control.set(state);
            configs.push(config);
        }
        let mut success = configs[0]
            .daemon
            .control
            .begin_request(configs[0].daemon.control.generation(), 1);
        success.responding(true);
        drop(success);
        drop(
            configs[1]
                .daemon
                .control
                .begin_request(configs[1].daemon.control.generation(), 2),
        );
        configs[0].daemon.api_pool = Arc::new(ApiPool::new(
            "chain0".into(),
            vec![ApiEndpoint {
                url: "https://example.invalid/rpc".into(),
                control: Arc::new(Control::default()),
                retry_after: Default::default(),
            }],
            None,
        ));
        let names = vec![
            "Short@".into(),
            "A much longer chain identity@".into(),
            "Disabled@".into(),
        ];
        let sources = vec![(0, None), (0, Some(0)), (1, None), (2, None)];
        let theme = Theme::desert(crate::cli::EmojiMode::Off);
        for width in [80, 110, 160] {
            let mut terminal =
                Terminal::new(ratatui::backend::TestBackend::new(width, 35)).unwrap();
            terminal
                .draw(|frame| {
                    render_rows(
                        frame,
                        &theme,
                        &configs,
                        &sources,
                        &names,
                        0,
                        0,
                        "",
                        &mut Activity::default(),
                    )
                })
                .unwrap();
            let buffer = terminal.backend().buffer();
            let rows: Vec<String> = buffer
                .content
                .chunks(width as usize)
                .map(|row| row.iter().map(|cell| cell.symbol()).collect())
                .collect();
            let header = rows
                .iter()
                .position(|row| {
                    row.contains("Chain") && row.contains("Enabled") && row.contains("Monitor")
                })
                .unwrap();
            let source_col = column(&rows[header], "Source").unwrap();
            let enabled_col = column(&rows[header], "Enabled").unwrap();
            let monitor_col = column(&rows[header], "Monitor").unwrap();
            let communication_col = column(&rows[header], "Connection").unwrap();
            // The header has a one-row gap; all following source rows use the
            // same columns even with different chain names and source types.
            let first = header + 2;
            assert_eq!(buffer[(1, first as u16)].symbol(), SELECTION_MARKER);
            assert_eq!(buffer[(1, first as u16)].bg, theme.accent);
            assert_eq!(buffer[(1, first as u16 + 1)].symbol(), " ");
            for (offset, kind) in ["Daemon", "API", "Daemon", "Daemon"]
                .into_iter()
                .enumerate()
            {
                assert_eq!(
                    column(&rows[first + offset], kind),
                    Some(source_col),
                    "{width}: {rows:#?}"
                );
                assert_eq!(
                    column(
                        &rows[first + offset],
                        if offset == 3 { "- No" } else { "+ Yes" }
                    ),
                    Some(enabled_col),
                    "{width}: {rows:#?}"
                );
            }
            assert_eq!(
                column(&rows[first + 2], "~ Yes"),
                Some(monitor_col),
                "{width}: {rows:#?}"
            );
            assert_eq!(
                column(&rows[first], "Responding"),
                Some(communication_col),
                "{width}: {rows:#?}"
            );
            assert_eq!(
                column(&rows[first + 1], "Not used"),
                Some(communication_col),
                "{width}: {rows:#?}"
            );
            assert_eq!(
                column(&rows[first + 2], "Unavailable"),
                Some(communication_col),
                "{width}: {rows:#?}"
            );
            assert_eq!(
                buffer[(communication_col as u16, first as u16)].fg,
                theme.positive
            );
            assert!(rows.iter().any(|row| row.contains("Directory:")));
            assert!(rows.iter().any(|row| row.contains("Identity: iTestSystem")));
            if width >= 112 {
                let endpoint_col = column(&rows[header], "Endpoint").unwrap();
                assert_eq!(column(&rows[first], "http://"), Some(endpoint_col));
                assert_eq!(column(&rows[first + 1], "https://"), Some(endpoint_col));
            }
        }
    }

    #[test]
    fn idle_and_unused_connections_are_distinct_from_failed_requests() {
        let mut status = Communication::default();
        let ttl = Duration::from_secs(30);
        assert_eq!(
            communication_label(ConnectionState::Enabled, &status, ttl).0,
            "Not used"
        );
        status.outcome = Some(CommunicationOutcome::Responding);
        status.last_contact = Some(std::time::Instant::now() - Duration::from_secs(60));
        assert_eq!(
            communication_label(ConnectionState::Enabled, &status, ttl).0,
            "Idle"
        );
        status.outcome = Some(CommunicationOutcome::Failed);
        assert_eq!(
            communication_label(ConnectionState::Enabled, &status, ttl).0,
            "Unavailable"
        );
        assert_eq!(
            communication_label(ConnectionState::Disabled, &status, ttl).0,
            "—"
        );
        status.in_flight.insert(1);
        assert_eq!(
            communication_label(ConnectionState::Monitored, &status, ttl).0,
            "Working"
        );
        assert!(mode_description(ConnectionState::Monitored).contains("enabled"));
    }
}
