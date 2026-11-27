use crate::{
    config::Config,
    sources::Registry,
    theme::Theme,
    zmq::setup::{self, Survey},
};
use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    buffer::Buffer,
    layout::{Position, Rect},
    style::Stylize,
    text::Line,
    widgets::{Block, Borders, Clear, Paragraph, Widget, Wrap},
};
use std::path::PathBuf;

pub(in crate::app) const CLIPBOARD_LABEL: &str = "ZMQ config lines";

#[derive(Clone, Copy, Debug)]
pub(in crate::app) enum CopyStatus {
    Copying,
    Copied,
    Failed,
}

pub(in crate::app) fn apply(survey: &Survey, configs: &[Config]) {
    for config in configs {
        if let Some(path) = &config.daemon.config_file
            && setup::local_host(&config.daemon.host)
        {
            config.daemon.control.zmq.configure(survey.settings(path));
        }
    }
}

pub(in crate::app) struct Modal {
    index: usize,
    path: PathBuf,
    port: String,
    pub editing: bool,
    generated: String,
    copy_area: Rect,
    message: String,
    result: Option<tokio::sync::oneshot::Receiver<(Survey, Result<String, String>)>>,
    last_status: Vec<String>,
    scroll: u16,
}

impl Modal {
    pub(in crate::app) fn open(index: usize, registry: &Registry) -> Option<Self> {
        let config = &registry.configs[index];
        if !setup::local_host(&config.daemon.host) {
            return None;
        }
        let mut modal = Self {
            index,
            path: config.daemon.config_file.clone()?,
            port: String::new(),
            editing: false,
            generated: String::new(),
            copy_area: Rect::ZERO,
            message: String::new(),
            result: None,
            last_status: Vec::new(),
            scroll: 0,
        };
        modal.reload(registry);
        Some(modal)
    }

    fn reload(&mut self, registry: &Registry) {
        let configs = registry.configs.clone();
        let saved = registry.settings_path.clone();
        let port = self.port.clone();
        let path = self.path.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.result = Some(receiver);
        self.generated.clear();
        self.message = "Checking daemon configuration and port availability…".into();
        tokio::task::spawn_blocking(move || {
            let survey = Survey::read(setup::known_paths(&configs, &saved));
            let suggestion = survey.suggestion(&path, &port);
            let _ = sender.send((survey, suggestion));
        });
    }

    pub(in crate::app) fn poll(&mut self, registry: &Registry) -> bool {
        use tokio::sync::oneshot::error::TryRecvError;
        let mut redraw = false;
        match self.result.as_mut().map(|receiver| receiver.try_recv()) {
            Some(Ok((survey, suggestion))) => {
                self.result = None;
                apply(&survey, &registry.configs);
                match suggestion {
                    Ok(lines) => {
                        self.generated = lines;
                        self.message = String::new();
                    }
                    Err(error) => self.message = error,
                }
                redraw = true;
            }
            Some(Err(TryRecvError::Closed)) => {
                self.result = None;
                self.message = "Config check interrupted. Press r to retry.".into();
                redraw = true;
            }
            _ => {}
        }
        let status = Self::status(&registry.configs[self.index]);
        if status != self.last_status {
            redraw = true;
            self.last_status = status;
        }
        redraw
    }

    fn status(config: &Config) -> Vec<String> {
        let control = &config.daemon.control;
        let feeds = control.zmq.feeds();
        if feeds.is_empty() {
            return vec!["Not configured".into()];
        }
        feeds
            .into_iter()
            .map(|(topic, endpoint, status)| {
                let status = if matches!(status, crate::zmq::FeedStatus::Conflict) {
                    status.label()
                } else if control.state() == crate::sources::ConnectionState::Disabled {
                    "Source disabled".into()
                } else if !control.polling() {
                    "Source inactive".into()
                } else {
                    status.label()
                };
                format!("{} · {endpoint} · {status}", topic.name())
            })
            .collect()
    }

    pub(in crate::app) fn paste(&mut self, text: &str) {
        if self.editing {
            self.port.extend(
                text.chars()
                    .filter(char::is_ascii_digit)
                    .take(5usize.saturating_sub(self.port.len())),
            );
        }
    }

    pub(in crate::app) fn key(
        &mut self,
        code: KeyCode,
        registry: &Registry,
    ) -> (bool, Option<String>) {
        if self.editing {
            match code {
                KeyCode::Esc => self.editing = false,
                KeyCode::Enter => {
                    self.editing = false;
                    self.reload(registry);
                }
                KeyCode::Backspace => {
                    self.port.pop();
                }
                KeyCode::Char(digit) if digit.is_ascii_digit() && self.port.len() < 5 => {
                    self.port.push(digit)
                }
                _ => {}
            }
            return (false, None);
        }
        match code {
            KeyCode::Esc | KeyCode::F(2) => return (true, None),
            KeyCode::Char('p') => {
                self.port.clear();
                self.editing = true;
                self.scroll = 0;
            }
            KeyCode::Char('c') if !self.generated.is_empty() => {
                return (false, Some(self.generated.clone()));
            }
            KeyCode::Char('r') => self.reload(registry),
            KeyCode::Up | KeyCode::Char('k') => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => self.scroll = self.scroll.saturating_add(1),
            _ => {}
        }
        (false, None)
    }

    pub(in crate::app) fn copy_at(&self, position: Position) -> Option<String> {
        (!self.editing && !self.generated.is_empty() && self.copy_area.contains(position))
            .then(|| self.generated.clone())
    }

    pub(in crate::app) fn render(
        &mut self,
        frame: &mut Frame<'_>,
        theme: &Theme,
        registry: &Registry,
        name: &str,
        mouse_capture: bool,
        copy_status: Option<CopyStatus>,
    ) {
        self.copy_area = Rect::ZERO;
        let screen = frame.area();
        let width = screen.width.min(93);
        let height = screen.height.min(23);
        let area = Rect::new(
            screen.x + (screen.width - width) / 2,
            screen.y + (screen.height - height) / 2,
            width,
            height,
        );
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" ZMQ Setup · {name} "))
            .style(theme.base())
            .border_style(theme.focused());
        let inner = block.inner(area);
        frame.render_widget(Clear, area);
        frame.render_widget(block, area);
        let footer = if self.editing {
            "Enter check port · Esc cancel"
        } else {
            "c copy config lines · p choose port · r Recheck · j/k scroll · Esc close"
        };
        let body = Rect {
            height: inner.height.saturating_sub(1),
            ..inner
        };
        let mut lines = vec![
            Line::from(self.path.display().to_string()).bold(),
            Line::from(
                "Paste the lines below into this file, restart this daemon, then press r to Recheck.",
            ),
            Line::from("The daemon build must include ZMQ. RPC remains available during setup."),
            Line::default(),
        ];
        if self.editing {
            lines.insert(
                0,
                Line::styled(
                    format!("New publisher port (blank for automatic): {}_", self.port),
                    theme.focused(),
                ),
            );
        }
        let snippet = if !self.editing && !self.generated.is_empty() && body.width >= 8 {
            lines.push(Line::from(if mouse_capture {
                "Click anywhere inside the box to copy the full snippet."
            } else {
                "Press c to copy, or use Shift-M to enable click-to-copy."
            }));
            let top = Paragraph::new(lines.clone())
                .wrap(Wrap { trim: false })
                .line_count(body.width);
            let width = body.width - 2;
            let paragraph = Paragraph::new(self.generated.as_str())
                .wrap(Wrap { trim: false })
                .style(theme.link());
            let height = paragraph.line_count(width - 2) as u16 + 2;
            lines.extend((0..height).map(|_| Line::default()));
            let (title, style) = match copy_status {
                Some(CopyStatus::Copying) => ("Copying…".into(), theme.link()),
                Some(CopyStatus::Copied) => (
                    format!("{} Copied", theme.glyph("✓", "+")),
                    theme.positive(),
                ),
                Some(CopyStatus::Failed) => ("Copy failed".into(), theme.warning()),
                None => (format!("{} Copy", theme.glyph("📋", "[=]")), theme.link()),
            };
            let widget = paragraph.block(
                Block::bordered()
                    .style(theme.base())
                    .border_style(style)
                    .title(Line::styled(format!(" {title} "), style)),
            );
            Some((top, width, height, widget))
        } else {
            None
        };
        if self.generated.is_empty() && self.message.is_empty() {
            lines.push(Line::from("Both raw publishers are configured."));
        }
        if !self.message.is_empty() {
            lines.push(Line::styled(self.message.as_str(), theme.warning()));
        }
        let settings = registry.configs[self.index].daemon.control.zmq.settings();
        lines.extend(
            settings
                .errors
                .iter()
                .map(|error| Line::styled(error, theme.warning())),
        );
        lines.push(Line::default());
        lines.extend(
            self.last_status
                .iter()
                .map(|status| Line::from(status.as_str())),
        );
        let content = Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .style(theme.base());
        let max_scroll = content
            .line_count(body.width.max(1))
            .saturating_sub(usize::from(body.height));
        let scroll = usize::from(self.scroll).min(max_scroll);
        frame.render_widget(content.scroll((scroll as u16, 0)), body);
        if let Some((top, width, height, widget)) = snippet {
            let first = top.max(scroll);
            let last = (top + usize::from(height)).min(scroll + usize::from(body.height));
            if first < last {
                let visible = Rect::new(
                    body.x + 1,
                    body.y + (first - scroll) as u16,
                    width,
                    (last - first) as u16,
                );
                let area = Rect::new(0, 0, width, height);
                let mut buffer = Buffer::empty(area);
                widget.render(area, &mut buffer);
                for y in 0..visible.height {
                    for x in 0..width {
                        frame.buffer_mut()[(visible.x + x, visible.y + y)] =
                            buffer[(x, (first - top) as u16 + y)].clone();
                    }
                }
                self.copy_area = visible;
            }
        }
        if inner.height > 0 {
            frame.render_widget(
                Line::styled(footer, theme.muted()),
                Rect {
                    y: body.bottom(),
                    height: 1,
                    ..inner
                },
            );
        }
    }
}
