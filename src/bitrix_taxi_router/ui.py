from __future__ import annotations

import json


def render_blank_page(*, initial_member_id: str | None = None) -> str:
    initial_member_id_json = json.dumps(initial_member_id or "", ensure_ascii=False)
    template = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NaimTech | Распределение</title>
  <script src="//api.bitrix24.tech/api/v1/"></script>
  <style>
    :root {
      color-scheme: light;
      --brand-blue: #2e7bf4;
      --menu-bg: #eaf1fb;
      --menu-text: #5c6d96;
      --canvas-text: #1f2a44;
      --canvas-subtle: #8a96b2;
      --border-soft: #dce6f5;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      min-height: 100vh;
      background: #ffffff;
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    body {
      color: var(--canvas-text);
    }

    .app-shell {
      min-height: 100vh;
      display: grid;
      grid-template-columns: 287px minmax(0, 1fr);
      grid-template-rows: 93px minmax(0, 1fr);
      grid-template-areas:
        "header header"
        "sidebar main";
    }

    .topbar {
      grid-area: header;
      display: flex;
      align-items: center;
      padding: 0 32px;
      background: var(--brand-blue);
      color: #ffffff;
    }

    .topbar-title {
      margin: 0;
      font-size: 30px;
      font-weight: 700;
      line-height: 1.2;
    }

    .sidebar {
      grid-area: sidebar;
      background: var(--menu-bg);
      padding: 46px 0 32px 20px;
      border-right: 1px solid rgba(46, 123, 244, 0.06);
    }

    .menu-label {
      margin: 0 0 14px 14px;
      font-size: 14px;
      line-height: 1.2;
      text-transform: uppercase;
      color: var(--menu-text);
      letter-spacing: 0.02em;
    }

    .menu-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
      padding: 0;
      margin: 0;
      list-style: none;
    }

    .menu-button {
      width: 100%;
      min-height: 42px;
      border: 0;
      background: transparent;
      color: var(--menu-text);
      border-radius: 10px 0 0 10px;
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 9px 14px 9px 10px;
      text-align: left;
      font: inherit;
      font-size: 18px;
      font-weight: 500;
      cursor: pointer;
      transition: background-color 0.15s ease, color 0.15s ease;
    }

    .menu-button:hover {
      background: rgba(46, 123, 244, 0.08);
    }

    .menu-button.is-active {
      background: var(--brand-blue);
      color: #ffffff;
    }

    .menu-icon {
      width: 24px;
      height: 24px;
      flex: 0 0 24px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }

    .menu-icon svg {
      width: 24px;
      height: 24px;
      stroke: currentColor;
      fill: none;
      stroke-width: 1.8;
      stroke-linecap: round;
      stroke-linejoin: round;
    }

    .canvas {
      grid-area: main;
      background: #ffffff;
      padding: 40px 48px;
    }

    .canvas-card {
      min-height: calc(100vh - 173px);
      border: 1px dashed var(--border-soft);
      border-radius: 20px;
      padding: 32px 36px;
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), #ffffff);
      display: flex;
      flex-direction: column;
      justify-content: flex-start;
    }

    .canvas-card.is-centered {
      align-items: center;
      justify-content: center;
      text-align: center;
      border-style: solid;
      border-color: transparent;
      background: #ffffff;
    }

    .distribution-empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      text-align: center;
      min-height: 100%;
    }

    .distribution-reference-view {
      display: flex;
      flex-direction: column;
      gap: 24px;
      min-height: 100%;
    }

    .distribution-reference-head {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .distribution-reference-title {
      margin: 0;
      font-size: 28px;
      line-height: 1.25;
      font-weight: 700;
      color: #1f2a44;
    }

    .distribution-reference-description {
      margin: 0;
      max-width: 880px;
      font-size: 15px;
      line-height: 1.6;
      color: var(--canvas-subtle);
    }

    .distribution-reference-description[hidden],
    .reference-status[hidden],
    .distribution-groups-panel[hidden] {
      display: none;
    }

    .distribution-groups-panel {
      display: flex;
      flex-direction: column;
      gap: 24px;
    }

    .distribution-create-card {
      width: 340px;
      min-height: 205px;
      border: 2px dashed #cdd6ea;
      border-radius: 10px;
      background: #fbfbfe;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 14px;
      padding: 24px;
      cursor: pointer;
      transition: border-color 0.15s ease, background-color 0.15s ease;
    }

    .distribution-create-card:hover {
      border-color: var(--brand-blue);
      background: #f8fbff;
    }

    .distribution-create-card:focus-visible {
      outline: 2px solid rgba(46, 123, 244, 0.35);
      outline-offset: 2px;
    }

    .distribution-create-plus {
      width: 56px;
      height: 56px;
      border-radius: 50%;
      border: 2px dashed #cdd6ea;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      color: var(--brand-blue);
      font-size: 34px;
      line-height: 1;
      font-weight: 500;
      background: #ffffff;
    }

    .distribution-create-label {
      font-size: 16px;
      line-height: 1.4;
      font-weight: 600;
      color: #333333;
      text-align: center;
    }

    .reference-status {
      padding: 14px 16px;
      border-radius: 14px;
      border: 1px solid var(--border-soft);
      background: #f8fbff;
      color: #39507c;
      font-size: 14px;
      line-height: 1.5;
    }

    .reference-status.is-error {
      border-color: rgba(214, 69, 69, 0.2);
      background: #fff5f5;
      color: #a83a3a;
    }

    .reference-status.is-success {
      border-color: rgba(46, 123, 244, 0.16);
      background: rgba(46, 123, 244, 0.06);
      color: #255ec0;
    }

    .reference-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
    }

    .reference-grid[hidden] {
      display: none;
    }

    .reference-card {
      min-width: 0;
      border: 1px solid var(--border-soft);
      border-radius: 18px;
      background: #ffffff;
      padding: 20px;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .reference-card-head {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }

    .reference-card-title {
      margin: 0;
      font-size: 20px;
      line-height: 1.3;
      font-weight: 700;
      color: #1f2a44;
    }

    .reference-card-description {
      margin: 0;
      font-size: 14px;
      line-height: 1.5;
      color: var(--canvas-subtle);
    }

    .reference-list {
      display: flex;
      flex-direction: column;
      gap: 10px;
      padding: 0;
      margin: 0;
      list-style: none;
    }

    .reference-item {
      display: flex;
      flex-direction: column;
      gap: 4px;
      padding: 12px 14px;
      border-radius: 14px;
      background: #f7faff;
      border: 1px solid rgba(46, 123, 244, 0.08);
    }

    .reference-item-title {
      font-size: 15px;
      line-height: 1.4;
      font-weight: 600;
      color: #24324f;
      word-break: break-word;
    }

    .reference-item-meta {
      font-size: 13px;
      line-height: 1.5;
      color: #7081a8;
      word-break: break-word;
    }

    .reference-pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }

    .reference-pill {
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(46, 123, 244, 0.08);
      color: var(--brand-blue);
      font-size: 12px;
      line-height: 1.3;
      font-weight: 600;
    }

    .reference-pill.is-muted {
      background: rgba(92, 109, 150, 0.08);
      color: #5c6d96;
    }

    .distribution-form {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .distribution-form[hidden] {
      display: none;
    }

    .distribution-row {
      display: grid;
      grid-template-columns: 250px minmax(0, 1fr);
      gap: 12px;
      align-items: start;
      padding: 3px 8px 8px 0;
      border: 1px solid #ecf1f9;
      border-radius: 10px;
      background: #ffffff;
    }

    .distribution-row--simple {
      align-items: center;
      min-height: 47px;
      padding-bottom: 3px;
    }

    .distribution-row-label {
      display: flex;
      align-items: center;
      min-height: 38px;
      padding: 8px 0 8px 18px;
      font-size: 16px;
      line-height: 1.3;
      font-weight: 700;
      color: #333333;
      word-break: break-word;
    }

    .distribution-row-control {
      min-width: 0;
      padding-top: 5px;
    }

    .distribution-field {
      display: flex;
      flex-direction: column;
      gap: 8px;
      min-width: 0;
    }

    .distribution-label,
    .distribution-inline-label {
      font-size: 16px;
      line-height: 1.3;
      font-weight: 500;
      color: #333333;
    }

    .distribution-section-description {
      margin: 0;
      font-size: 12px;
      line-height: 1.4;
      color: #5c5c5c;
    }

    .distribution-input,
    .distribution-select {
      width: 100%;
      height: 38px;
      padding: 8px 18px;
      border: 2px solid #cdd6ea;
      border-radius: 10px;
      background: #ffffff;
      color: #333333;
      font: inherit;
      font-size: 16px;
      opacity: 0.8;
    }

    .distribution-input--compact {
      width: 96px;
      padding-left: 12px;
      padding-right: 12px;
      text-align: left;
    }

    .distribution-input:disabled,
    .distribution-select:disabled {
      background: #f6f8fc;
      color: #94a0bb;
    }

    .distribution-toggle {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      font-size: 14px;
      line-height: 1.4;
      font-weight: 600;
      color: #24324f;
    }

    .distribution-toggle input {
      width: 18px;
      height: 18px;
      margin: 0;
    }

    .participants-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin-bottom: 8px;
      padding-top: 3px;
    }

    .participants-toolbar .distribution-select {
      width: 138px;
      padding-left: 12px;
      padding-right: 12px;
    }

    .secondary-action {
      min-width: 111px;
      height: 38px;
      border: 0;
      border-radius: 10px;
      background: var(--brand-blue);
      color: #ffffff;
      font: inherit;
      font-size: 18px;
      font-weight: 500;
      cursor: pointer;
    }

    .participant-list,
    .checkbox-list {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .distribution-scroll-box {
      border: 1px solid var(--border-soft);
      border-radius: 10px;
      background: #fbfbfe;
      padding: 8px 18px;
      overflow-y: auto;
    }

    .distribution-scroll-box--participants {
      min-height: 112px;
      max-height: 170px;
    }

    .distribution-scroll-box--stages {
      min-height: 112px;
      max-height: 155px;
    }

    .load-stages-layout {
      display: grid;
      grid-template-columns: 296px minmax(0, 1fr);
      gap: 16px;
      align-items: stretch;
    }

    .load-stages-note {
      border: 1px solid var(--border-soft);
      border-radius: 10px;
      background: #fbfbfe;
      padding: 12px;
      min-height: 112px;
      display: flex;
      align-items: center;
    }

    .participant-row,
    .checkbox-row {
      display: grid;
      gap: 12px;
      align-items: center;
      padding: 3px 0;
      border: 0;
      border-radius: 0;
      background: transparent;
    }

    .participant-row {
      grid-template-columns: minmax(0, 1fr) 96px;
    }

    .participant-main,
    .checkbox-main {
      display: flex;
      gap: 10px;
      align-items: flex-start;
      min-width: 0;
    }

    .participant-main input,
    .checkbox-main input {
      width: 20px;
      height: 20px;
      margin-top: 0;
      flex: 0 0 20px;
    }

    .participant-info,
    .checkbox-info {
      display: flex;
      flex-direction: column;
      gap: 3px;
      min-width: 0;
    }

    .participant-name,
    .checkbox-name {
      font-size: 16px;
      line-height: 1.4;
      font-weight: 500;
      color: #333333;
      word-break: break-word;
    }

    .participant-meta,
    .checkbox-meta {
      font-size: 12px;
      line-height: 1.4;
      color: #7081a8;
      word-break: break-word;
    }

    .participant-limit {
      display: flex;
      flex-direction: column;
      gap: 0;
    }

    .participant-limit .distribution-input {
      width: 96px;
      padding-left: 12px;
      padding-right: 12px;
      text-align: left;
    }

    .participant-limit-label {
      display: none;
    }

    .distribution-actions {
      display: block;
    }

    .canvas-title {
      margin: 0;
      font-size: 32px;
      line-height: 1.2;
      font-weight: 700;
    }

    .canvas-subtitle {
      margin: 12px 0 0;
      max-width: 620px;
      font-size: 16px;
      line-height: 1.6;
      color: var(--canvas-subtle);
    }

    .section-badge {
      display: inline-flex;
      align-items: center;
      width: fit-content;
      margin-top: 24px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(46, 123, 244, 0.08);
      color: var(--brand-blue);
      font-size: 13px;
      font-weight: 600;
      letter-spacing: 0.01em;
    }

    .distribution-empty-title {
      margin: 0 0 18px;
      font-size: 24px;
      line-height: 1.3;
      font-weight: 700;
      color: #333333;
    }

    .primary-action {
      width: 100%;
      min-width: 0;
      height: 42px;
      border: 0;
      border-radius: 10px;
      background: var(--brand-blue);
      color: #ffffff;
      font: inherit;
      font-size: 18px;
      font-weight: 500;
      cursor: pointer;
      transition: opacity 0.15s ease;
    }

    .primary-action:hover {
      opacity: 0.92;
    }

    .distribution-checkbox-row {
      display: inline-flex;
      align-items: center;
      gap: 12px;
      min-height: 38px;
      font-size: 16px;
      line-height: 1.3;
      color: #333333;
    }

    .distribution-checkbox-row input {
      width: 20px;
      height: 20px;
      margin: 0;
    }

    .stats-view {
      display: flex;
      flex-direction: column;
      gap: 20px;
      min-height: 100%;
    }

    .stats-toolbar {
      display: flex;
      justify-content: flex-end;
    }

    .stats-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }

    .stats-grid--full {
      grid-template-columns: minmax(0, 1fr);
    }

    .stats-empty {
      padding: 16px;
      border-radius: 14px;
      background: #f7faff;
      border: 1px dashed rgba(46, 123, 244, 0.16);
      color: #7081a8;
      font-size: 14px;
      line-height: 1.5;
    }

    .section-panel[hidden] {
      display: none;
    }

    @media (max-width: 900px) {
      .app-shell {
        grid-template-columns: 1fr;
        grid-template-rows: 93px auto minmax(0, 1fr);
        grid-template-areas:
          "header"
          "sidebar"
          "main";
      }

      .sidebar {
        padding: 24px 20px;
      }

      .menu-button {
        border-radius: 10px;
      }

      .canvas {
        padding: 24px 20px 32px;
      }

      .canvas-card {
        min-height: 360px;
      }

      .reference-grid {
        grid-template-columns: 1fr;
      }

      .distribution-row,
      .participant-row,
      .load-stages-layout {
        grid-template-columns: 1fr;
      }

      .distribution-row {
        padding: 8px;
      }

      .distribution-row-label,
      .distribution-row-control {
        padding: 0;
      }

      .stats-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <header class="topbar">
      <h1 class="topbar-title">NaimTech | Распределение</h1>
    </header>

    <aside class="sidebar" aria-label="Меню разделов">
      <p class="menu-label">Меню</p>
      <ul class="menu-list">
        <li>
          <button class="menu-button is-active" type="button" data-view="overview">
            <span class="menu-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <rect x="4" y="4" width="6" height="6" rx="1"></rect>
                <rect x="14" y="4" width="6" height="6" rx="1"></rect>
                <rect x="4" y="14" width="6" height="6" rx="1"></rect>
                <rect x="14" y="14" width="6" height="6" rx="1"></rect>
              </svg>
            </span>
            <span>Общая информация</span>
          </button>
        </li>
        <li>
          <button class="menu-button" type="button" data-view="distribution">
            <span class="menu-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <polyline points="4 14 9 9 13 13 20 6"></polyline>
                <polyline points="16 6 20 6 20 10"></polyline>
                <path d="M4 4h5v5H4z"></path>
              </svg>
            </span>
            <span>Распределение сделок</span>
          </button>
        </li>
        <li>
          <button class="menu-button" type="button" data-view="stats">
            <span class="menu-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <path d="M12 20a8 8 0 1 0-8-8"></path>
                <path d="M12 16a4 4 0 1 0-4-4"></path>
                <circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none"></circle>
              </svg>
            </span>
            <span>Статистика</span>
          </button>
        </li>
        <li>
          <button class="menu-button" type="button" data-view="settings">
            <span class="menu-icon" aria-hidden="true">
              <svg viewBox="0 0 24 24">
                <circle cx="12" cy="12" r="3.25"></circle>
                <path d="M19.4 15a1 1 0 0 0 .2 1.1l.1.1a2 2 0 0 1-2.8 2.8l-.1-.1a1 1 0 0 0-1.1-.2 1 1 0 0 0-.6.9V20a2 2 0 0 1-4 0v-.2a1 1 0 0 0-.6-.9 1 1 0 0 0-1.1.2l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1 1 0 0 0 .2-1.1 1 1 0 0 0-.9-.6H4a2 2 0 0 1 0-4h.2a1 1 0 0 0 .9-.6 1 1 0 0 0-.2-1.1l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1 1 0 0 0 1.1.2h.1a1 1 0 0 0 .6-.9V4a2 2 0 0 1 4 0v.2a1 1 0 0 0 .6.9h.1a1 1 0 0 0 1.1-.2l.1-.1a2 2 0 0 1 2.8 2.8l-.1.1a1 1 0 0 0-.2 1.1v.1a1 1 0 0 0 .9.6H20a2 2 0 0 1 0 4h-.2a1 1 0 0 0-.9.6z"></path>
              </svg>
            </span>
            <span>Настройки</span>
          </button>
        </li>
      </ul>
    </aside>

    <main class="canvas">
      <section class="canvas-card" aria-live="polite">
        <div class="section-panel" id="defaultPanel">
          <span class="section-badge" id="sectionBadge">Раздел</span>
          <h2 class="canvas-title" id="sectionTitle">Общая информация</h2>
          <p class="canvas-subtitle" id="sectionDescription">
            Собираем общую информацию. Пока собрали только то, что её нет.
          </p>
        </div>

        <div class="section-panel" id="statsPanel" hidden>
          <div class="stats-view">
            <div class="distribution-reference-head">
              <h2 class="distribution-reference-title">Статистика</h2>
              <p class="distribution-reference-description">
                Здесь отображается технический журнал распределения: какие сделки обрабатывались, какой статус получили и кто назначался последним.
              </p>
            </div>

            <div class="stats-toolbar">
              <button class="secondary-action" id="refreshStatsButton" type="button">Обновить журнал</button>
            </div>

            <div class="reference-status" id="statsStatus" hidden>
              Откройте раздел, чтобы загрузить журнал.
            </div>

            <div class="stats-grid">
              <section class="reference-card" aria-labelledby="statsSummaryTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="statsSummaryTitle">Сводка</h3>
                  <p class="reference-card-description">Короткое состояние рантайма по последним записям.</p>
                </div>
                <div class="reference-list" id="statsSummaryList"></div>
              </section>

              <section class="reference-card" aria-labelledby="statsMembersTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="statsMembersTitle">Участники</h3>
                  <p class="reference-card-description">Последнее назначение по каждому участнику из runtime-таблицы.</p>
                </div>
                <div class="reference-list" id="statsMembersList"></div>
              </section>
            </div>

            <div class="stats-grid stats-grid--full">
              <section class="reference-card" aria-labelledby="statsDiagnosticsTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="statsDiagnosticsTitle">Диагностический журнал</h3>
                  <p class="reference-card-description">Сохраненные backend-события: bind, hit на endpoint, ошибки и причины остановки.</p>
                </div>
                <div class="reference-list" id="statsDiagnosticsList"></div>
              </section>

              <section class="reference-card" aria-labelledby="statsJournalTitle">
                <div class="reference-card-head">
                  <h3 class="reference-card-title" id="statsJournalTitle">Журнал сделок</h3>
                  <p class="reference-card-description">Последние записи из runtime-журнала распределения по сделкам.</p>
                </div>
                <div class="reference-list" id="statsJournalList"></div>
              </section>
            </div>
          </div>
        </div>

        <div class="section-panel" id="distributionPanel" hidden>
          <div class="distribution-reference-view">
            <div class="distribution-reference-head">
              <h2 class="distribution-reference-title">Распределение сделок</h2>
              <p class="distribution-reference-description" hidden></p>
            </div>

            <div class="distribution-groups-panel" id="distributionGroupsPanel">
              <button class="distribution-create-card" id="createDistributionGroupButton" type="button">
                <span class="distribution-create-plus" aria-hidden="true">+</span>
                <span class="distribution-create-label">Добавить новую группу</span>
              </button>
            </div>

            <div class="reference-status" id="distributionStatus" hidden>
              Откройте раздел, чтобы загрузить конфигурацию группы.
            </div>

            <form class="distribution-form" id="distributionForm" hidden>
              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Наименование группы распределения:</span>
                <span class="distribution-row-control">
                  <input class="distribution-input" id="groupNameInput" type="text" placeholder="Например, Основная группа распределения">
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Тип распределения:</span>
                <span class="distribution-row-control">
                  <select class="distribution-select" id="distributionTypeSelect">
                    <option value="round_robin_load_time">По очереди с лимитами по нагрузке и времени</option>
                  </select>
                </span>
              </label>

              <section class="distribution-row" aria-label="Участники распределения">
                <div class="distribution-row-label">Участники распределения:</div>
                <div class="distribution-row-control">
                  <div class="participants-toolbar">
                    <span class="distribution-inline-label">Массовое заполнение лимита для:</span>
                    <select class="distribution-select" id="bulkLimitScopeSelect">
                      <option value="selected">Выбранных</option>
                      <option value="all">Всех</option>
                    </select>
                    <span class="distribution-inline-label">Лимит:</span>
                    <input class="distribution-input distribution-input--compact" id="bulkLimitValueInput" type="number" min="0" step="1" placeholder="0">
                    <button class="secondary-action" id="applyBulkLimitButton" type="button">Применить</button>
                  </div>
                  <div class="distribution-scroll-box distribution-scroll-box--participants">
                    <div class="participant-list" id="participantsList"></div>
                  </div>
                </div>
              </section>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Отслеживаемое событие:</span>
                <span class="distribution-row-control">
                  <select class="distribution-select" id="eventTypeSelect">
                    <option value="deal_created">Создание новой сделки</option>
                  </select>
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Статус для распределения:</span>
                <span class="distribution-row-control">
                  <select class="distribution-select" id="distributionStageSelect"></select>
                </span>
              </label>

              <section class="distribution-row" aria-labelledby="loadStagesTitle">
                <div class="distribution-row-label" id="loadStagesTitle">Статусы для определения нагрузки:</div>
                <div class="distribution-row-control">
                  <div class="load-stages-layout">
                    <div class="load-stages-note">
                      <p class="distribution-section-description">
                        Вы выбрали режим распределения по нагрузке на менеджера, поэтому в данном поле следует указать, в каких
                        стадиях активные сделки являются нагрузкой для выбранных менеджеров.
                      </p>
                    </div>
                    <div class="distribution-scroll-box distribution-scroll-box--stages">
                      <div class="checkbox-list" id="loadStagesList"></div>
                    </div>
                  </div>
                </div>
              </section>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Поле для установки/определения ответственного:</span>
                <span class="distribution-row-control">
                  <select class="distribution-select" id="responsibleFieldSelect"></select>
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Время ожидания, сек</span>
                <span class="distribution-row-control">
                  <input class="distribution-input" id="waitSecondsInput" type="number" min="1" step="1" placeholder="0">
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Интервал перепроверки при переполнении лимитов:</span>
                <span class="distribution-row-control">
                  <input class="distribution-input" id="retryIntervalInput" type="number" min="1" step="1" placeholder="Например, 30">
                </span>
              </label>

              <label class="distribution-row distribution-row--simple">
                <span class="distribution-row-label">Группа активна:</span>
                <span class="distribution-row-control">
                  <span class="distribution-checkbox-row">
                    <input id="groupActiveInput" type="checkbox">
                    <span>Активна</span>
                  </span>
                </span>
              </label>

              <div class="distribution-actions">
                <button class="primary-action" id="saveDistributionButton" type="button">Применить</button>
              </div>
            </form>
          </div>
        </div>
      </section>
    </main>
  </div>

  <script>
    const sectionContent = {
      overview: {
        badge: "Раздел",
        title: "Общая информация",
        description: "Собираем общую информацию. Пока собрали только то, что её нет.",
      },
      distribution: {
        badge: "Раздел",
        title: "Распределение сделок",
        description: "",
      },
      stats: {
        badge: "Раздел",
        title: "Статистика",
        description: "Технический журнал событий и назначений по сделкам.",
      },
      settings: {
        badge: "Раздел",
        title: "Настройки",
        description: "Конфигурация пространства-времени. Пока настроено только пространство. Времени на настройку не хватило.",
      },
    };

    const defaultPanel = document.getElementById("defaultPanel");
    const statsPanel = document.getElementById("statsPanel");
    const distributionPanel = document.getElementById("distributionPanel");
    const sectionBadge = document.getElementById("sectionBadge");
    const sectionTitle = document.getElementById("sectionTitle");
    const sectionDescription = document.getElementById("sectionDescription");
    const mainCard = document.querySelector(".canvas > .canvas-card");
    const menuButtons = document.querySelectorAll("[data-view]");
    const distributionStatus = document.getElementById("distributionStatus");
    const distributionGroupsPanel = document.getElementById("distributionGroupsPanel");
    const createDistributionGroupButton = document.getElementById("createDistributionGroupButton");
    const distributionForm = document.getElementById("distributionForm");
    const groupNameInput = document.getElementById("groupNameInput");
    const distributionTypeSelect = document.getElementById("distributionTypeSelect");
    const eventTypeSelect = document.getElementById("eventTypeSelect");
    const distributionStageSelect = document.getElementById("distributionStageSelect");
    const responsibleFieldSelect = document.getElementById("responsibleFieldSelect");
    const waitSecondsInput = document.getElementById("waitSecondsInput");
    const retryIntervalInput = document.getElementById("retryIntervalInput");
    const groupActiveInput = document.getElementById("groupActiveInput");
    const bulkLimitScopeSelect = document.getElementById("bulkLimitScopeSelect");
    const bulkLimitValueInput = document.getElementById("bulkLimitValueInput");
    const applyBulkLimitButton = document.getElementById("applyBulkLimitButton");
    const participantsList = document.getElementById("participantsList");
    const loadStagesList = document.getElementById("loadStagesList");
    const saveDistributionButton = document.getElementById("saveDistributionButton");
    const statsStatus = document.getElementById("statsStatus");
    const statsSummaryList = document.getElementById("statsSummaryList");
    const statsMembersList = document.getElementById("statsMembersList");
    const statsDiagnosticsList = document.getElementById("statsDiagnosticsList");
    const statsJournalList = document.getElementById("statsJournalList");
    const refreshStatsButton = document.getElementById("refreshStatsButton");
    const initialDistributionMemberId = __INITIAL_MEMBER_ID__;
    const distributionState = {
      isLoaded: false,
      isLoading: false,
      memberId: initialDistributionMemberId || new URLSearchParams(window.location.search).get("member_id") || "",
      auth: null,
      referenceData: null,
      config: null,
      openFormRequested: false,
    };
    const statsState = {
      isLoaded: false,
      isLoading: false,
      data: null,
    };

    function setDistributionStatus(message, tone) {
      distributionStatus.hidden = false;
      distributionStatus.textContent = message;
      distributionStatus.classList.remove("is-error", "is-success");
      if (tone) {
        distributionStatus.classList.add(tone);
      }
    }

    function showDistributionLanding() {
      distributionGroupsPanel.hidden = false;
      distributionStatus.hidden = true;
      distributionForm.hidden = true;
    }

    function showDistributionForm() {
      distributionGroupsPanel.hidden = true;
      distributionStatus.hidden = false;
    }

    function setStatsStatus(message, tone) {
      statsStatus.hidden = false;
      statsStatus.textContent = message;
      statsStatus.classList.remove("is-error", "is-success");
      if (tone) {
        statsStatus.classList.add(tone);
      }
    }

    function createStatsItem(title, metaLines, pills) {
      const item = document.createElement("div");
      item.className = "reference-item";

      const itemTitle = document.createElement("div");
      itemTitle.className = "reference-item-title";
      itemTitle.textContent = title;
      item.appendChild(itemTitle);

      (metaLines || []).filter(Boolean).forEach((line) => {
        const meta = document.createElement("div");
        meta.className = "reference-item-meta";
        meta.textContent = line;
        item.appendChild(meta);
      });

      if (Array.isArray(pills) && pills.length) {
        const pillRow = document.createElement("div");
        pillRow.className = "reference-pill-row";
        pills.forEach((pill) => {
          const badge = document.createElement("span");
          badge.className = `reference-pill${pill.muted ? " is-muted" : ""}`;
          badge.textContent = pill.label;
          pillRow.appendChild(badge);
        });
        item.appendChild(pillRow);
      }

      return item;
    }

    function renderStatsEmpty(container, message) {
      container.innerHTML = "";
      const empty = document.createElement("div");
      empty.className = "stats-empty";
      empty.textContent = message;
      container.appendChild(empty);
    }

    function formatStatsDate(value) {
      if (!value) {
        return "нет данных";
      }
      const parsed = new Date(value);
      if (Number.isNaN(parsed.getTime())) {
        return value;
      }
      return parsed.toLocaleString("ru-RU");
    }

    function renderStatsData(payload) {
      const summary = payload && payload.summary ? payload.summary : {};
      const diagnostics = Array.isArray(payload && payload.diagnostics) ? payload.diagnostics : [];
      const journal = Array.isArray(payload && payload.journal) ? payload.journal : [];
      const members = Array.isArray(payload && payload.members) ? payload.members : [];

      statsSummaryList.innerHTML = "";
      statsSummaryList.appendChild(createStatsItem("Записей в журнале", [`Всего: ${summary.journal_count || 0}`], [
        { label: `Назначено: ${summary.assigned_count || 0}` },
        { label: `Ожидание: ${summary.waiting_count || 0}`, muted: true },
        { label: `Игнорировано: ${summary.ignored_count || 0}`, muted: true },
      ]));
      statsSummaryList.appendChild(createStatsItem("Runtime участников", [`Записей: ${summary.member_runtime_count || 0}`], []));
      statsSummaryList.appendChild(createStatsItem("Диагностических записей", [`Всего: ${summary.diagnostic_count || 0}`], []));

      if (!members.length) {
        renderStatsEmpty(statsMembersList, "Пока нет runtime-записей по участникам.");
      } else {
        statsMembersList.innerHTML = "";
        members.forEach((member) => {
          statsMembersList.appendChild(createStatsItem(
            member.user_name || `Пользователь ${member.user_id}`,
            [
              `ID: ${member.user_id}`,
              `Последняя сделка: ${member.last_assigned_deal_id || "не было"}`,
              `Последнее назначение: ${formatStatsDate(member.last_assigned_at)}`,
              `Обновлено: ${formatStatsDate(member.updated_at)}`,
            ],
            [],
          ));
        });
      }

      if (!diagnostics.length) {
        renderStatsEmpty(statsDiagnosticsList, "Диагностических записей пока нет.");
      } else {
        statsDiagnosticsList.innerHTML = "";
        diagnostics.forEach((item) => {
          const detailLines = [];
          if (item.deal_id) {
            detailLines.push(`Сделка: #${item.deal_id}`);
          }
          if (item.details && Object.keys(item.details).length) {
            detailLines.push(`Детали: ${JSON.stringify(item.details)}`);
          }
          detailLines.push(`Источник: ${item.source}`);
          detailLines.push(`Время: ${formatStatsDate(item.created_at)}`);
          statsDiagnosticsList.appendChild(createStatsItem(
            item.message || "Диагностическое событие",
            detailLines,
            [
              { label: item.level || "info", muted: item.level !== "error" },
            ],
          ));
        });
      }

      if (!journal.length) {
        renderStatsEmpty(statsJournalList, "Журнал пуст. Как только backend запишет обработку сделок, записи появятся здесь.");
      } else {
        statsJournalList.innerHTML = "";
        journal.forEach((item) => {
          const assignedTo = item.assigned_user_id
            ? `${item.assigned_user_name || item.assigned_user_id} (${item.assigned_user_id})`
            : "не назначено";
          statsJournalList.appendChild(createStatsItem(
            `Сделка #${item.deal_id}`,
            [
              `Статус: ${item.status}`,
              `Назначено: ${assignedTo}`,
              `Поле: ${item.assigned_field_id || "не заполнялось"}`,
              `Комментарий: ${item.note || "нет"}`,
              `Обновлено: ${formatStatsDate(item.updated_at)}`,
            ],
            [
              { label: item.event_type || "event" },
            ],
          ));
        });
      }
    }

    async function fetchJson(url, options) {
      const response = await fetch(url, options);
      const responseText = await response.text();
      let payload = {};
      if (responseText) {
        try {
          payload = JSON.parse(responseText);
        } catch (error) {
          payload = {};
        }
      }
      if (!response.ok) {
        throw new Error(payload.detail || responseText || `Backend returned HTTP ${response.status}.`);
      }
      return payload;
    }

    function createDefaultDistributionConfig(referenceData) {
      const defaultResponsibleField = (referenceData.responsible_fields || []).find((field) => field.is_default)
        || (referenceData.responsible_fields || [])[0]
        || null;
      return {
        name: "",
        distribution_type: "round_robin_load_time",
        event_type: "deal_created",
        distribution_stage_id: "",
        responsible_field_id: defaultResponsibleField ? defaultResponsibleField.id : "",
        wait_seconds: "",
        retry_interval_seconds: "",
        is_active: true,
        members: [],
        load_stage_ids: [],
      };
    }

    function normalizeLoadedDistributionConfig(config, referenceData) {
      const defaults = createDefaultDistributionConfig(referenceData);
      if (!config) {
        return defaults;
      }

      const members = Array.isArray(config.members) ? config.members : [];
      const loadStageIds = Array.isArray(config.load_stage_ids) ? config.load_stage_ids : [];

      return {
        ...defaults,
        ...config,
        members,
        load_stage_ids: loadStageIds,
      };
    }

    function renderSelectOptions(selectElement, items, selectedValue, placeholder) {
      selectElement.innerHTML = "";

      if (placeholder) {
        const placeholderOption = document.createElement("option");
        placeholderOption.value = "";
        placeholderOption.textContent = placeholder;
        selectElement.appendChild(placeholderOption);
      }

      (items || []).forEach((item) => {
        const option = document.createElement("option");
        option.value = item.id;
        option.textContent = item.name || item.id;
        option.selected = item.id === selectedValue;
        selectElement.appendChild(option);
      });
    }

    function renderParticipants(config, users) {
      participantsList.innerHTML = "";
      const selectedMembers = new Map((config.members || []).map((member) => [member.user_id, member]));

      (users || []).forEach((user) => {
        const selectedMember = selectedMembers.get(user.id);
        const row = document.createElement("div");
        row.className = "participant-row";

        const label = document.createElement("label");
        label.className = "participant-main";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = Boolean(selectedMember);
        checkbox.dataset.userId = user.id;

        const info = document.createElement("span");
        info.className = "participant-info";

        const name = document.createElement("span");
        name.className = "participant-name";
        name.textContent = user.name || `Пользователь ${user.id}`;

        const meta = document.createElement("span");
        meta.className = "participant-meta";
        meta.textContent = `ID: ${user.id}${user.is_active ? " · активен" : " · неактивен"}`;

        info.append(name, meta);
        label.append(checkbox, info);

        const limitWrapper = document.createElement("label");
        limitWrapper.className = "participant-limit";

        const limitLabel = document.createElement("span");
        limitLabel.className = "participant-limit-label";
        limitLabel.textContent = "Лимит";

        const limitInput = document.createElement("input");
        limitInput.className = "distribution-input";
        limitInput.type = "number";
        limitInput.min = "0";
        limitInput.step = "1";
        limitInput.placeholder = "0";
        limitInput.value = selectedMember ? String(selectedMember.limit) : "";
        limitInput.dataset.limitFor = user.id;
        limitInput.disabled = !checkbox.checked;

        checkbox.addEventListener("change", () => {
          limitInput.disabled = !checkbox.checked;
          if (!checkbox.checked) {
            limitInput.value = "";
          }
        });

        limitWrapper.append(limitLabel, limitInput);
        row.append(label, limitWrapper);
        participantsList.appendChild(row);
      });
    }

    function renderLoadStages(config, stages) {
      loadStagesList.innerHTML = "";
      const selectedStageIds = new Set(config.load_stage_ids || []);

      (stages || []).forEach((stage) => {
        const row = document.createElement("label");
        row.className = "checkbox-row";

        const main = document.createElement("span");
        main.className = "checkbox-main";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = selectedStageIds.has(stage.id);
        checkbox.dataset.stageId = stage.id;

        const info = document.createElement("span");
        info.className = "checkbox-info";

        const name = document.createElement("span");
        name.className = "checkbox-name";
        name.textContent = stage.name || stage.id;

        const meta = document.createElement("span");
        meta.className = "checkbox-meta";
        meta.textContent = `ID: ${stage.id}${stage.sort !== undefined ? ` · sort: ${stage.sort}` : ""}`;

        info.append(name, meta);
        main.append(checkbox, info);
        row.appendChild(main);
        loadStagesList.appendChild(row);
      });
    }

    function renderDistributionConfigForm() {
      const referenceData = distributionState.referenceData || { users: [], stages: [], responsible_fields: [] };
      const config = distributionState.config || createDefaultDistributionConfig(referenceData);

      groupNameInput.value = config.name || "";
      distributionTypeSelect.value = config.distribution_type || "round_robin_load_time";
      eventTypeSelect.value = config.event_type || "deal_created";
      waitSecondsInput.value = config.wait_seconds === "" ? "" : String(config.wait_seconds ?? "");
      retryIntervalInput.value = config.retry_interval_seconds === "" ? "" : String(config.retry_interval_seconds ?? "");
      groupActiveInput.checked = Boolean(config.is_active);

      renderSelectOptions(distributionStageSelect, referenceData.stages, config.distribution_stage_id, "Выберите статус");
      renderSelectOptions(
        responsibleFieldSelect,
        referenceData.responsible_fields,
        config.responsible_field_id,
        "Выберите поле",
      );
      renderParticipants(config, referenceData.users);
      renderLoadStages(config, referenceData.stages);
      showDistributionForm();
      distributionForm.hidden = false;
    }

    function getBitrixAuth() {
      return new Promise((resolve) => {
        if (!window.BX24 || typeof window.BX24.init !== "function" || typeof window.BX24.getAuth !== "function") {
          resolve(null);
          return;
        }

        try {
          window.BX24.init(() => {
            const auth = window.BX24.getAuth();
            resolve(auth || null);
          });
        } catch (error) {
          console.error("BX24 auth initialization failed", error);
          resolve(null);
        }
      });
    }

    async function resolveBitrixAuth() {
      if (distributionState.auth) {
        return distributionState.auth;
      }

      const auth = await getBitrixAuth();
      distributionState.auth = auth;
      if (!distributionState.memberId && auth && auth.member_id) {
        distributionState.memberId = auth.member_id;
      }
      return distributionState.auth;
    }

    async function resolveDistributionMemberId() {
      if (distributionState.memberId) {
        return distributionState.memberId;
      }

      const auth = await resolveBitrixAuth();
      const bitrixMemberId = (auth && auth.member_id) || "";
      distributionState.memberId = bitrixMemberId;
      return distributionState.memberId;
    }

    async function syncPortalContextFromBitrix() {
      const auth = await resolveBitrixAuth();
      if (!auth || !auth.member_id || !auth.domain || !auth.access_token) {
        return;
      }

      const payload = {
        member_id: auth.member_id,
        DOMAIN: auth.domain,
        AUTH_ID: auth.access_token,
        REFRESH_ID: auth.refresh_token || "",
        PROTOCOL: "1",
      };

      const response = await fetch("/api/ui/groups/portal-context", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        throw new Error(errorPayload.detail || "Не удалось синхронизировать портал с backend.");
      }
    }

    function collectDistributionConfigFromForm() {
      const members = [];
      participantsList.querySelectorAll("[data-user-id]").forEach((checkbox) => {
        if (!checkbox.checked) {
          return;
        }

        const limitInput = participantsList.querySelector(`[data-limit-for="${checkbox.dataset.userId}"]`);
        members.push({
          user_id: checkbox.dataset.userId,
          limit: limitInput ? Number(limitInput.value || 0) : 0,
        });
      });

      const loadStageIds = [];
      loadStagesList.querySelectorAll("[data-stage-id]").forEach((checkbox) => {
        if (checkbox.checked) {
          loadStageIds.push(checkbox.dataset.stageId);
        }
      });

      return {
        name: groupNameInput.value.trim(),
        distribution_type: distributionTypeSelect.value,
        event_type: eventTypeSelect.value,
        distribution_stage_id: distributionStageSelect.value,
        responsible_field_id: responsibleFieldSelect.value,
        wait_seconds: Number(waitSecondsInput.value || 0),
        retry_interval_seconds: Number(retryIntervalInput.value || 0),
        is_active: groupActiveInput.checked,
        members,
        load_stage_ids: loadStageIds,
      };
    }

    function applyBulkLimitFromForm() {
      const bulkValue = bulkLimitValueInput.value.trim();
      if (!bulkValue) {
        setDistributionStatus("Укажите значение лимита для массового заполнения.", "is-error");
        return;
      }

      const shouldApplyToAll = bulkLimitScopeSelect.value === "all";
      participantsList.querySelectorAll("[data-user-id]").forEach((checkbox) => {
        if (!shouldApplyToAll && !checkbox.checked) {
          return;
        }

        const limitInput = participantsList.querySelector(`[data-limit-for="${checkbox.dataset.userId}"]`);
        if (!limitInput) {
          return;
        }
        if (!checkbox.checked && shouldApplyToAll) {
          checkbox.checked = true;
          limitInput.disabled = false;
        }
        limitInput.value = bulkValue;
      });

      setDistributionStatus("Лимиты обновлены в форме. Чтобы сохранить группу, нажмите «Применить».", "is-success");
    }

    async function saveDistributionConfig() {
      const distributionMemberId = await resolveDistributionMemberId();
      if (!distributionMemberId) {
        setDistributionStatus("Не удалось определить member_id портала для сохранения.", "is-error");
        return;
      }

      setDistributionStatus("Сохраняем конфигурацию группы распределения...");

      try {
        const payload = collectDistributionConfigFromForm();
        const response = await fetchJson(
          `/api/ui/groups/config?member_id=${encodeURIComponent(distributionMemberId)}`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(payload),
          },
        );

        distributionState.config = normalizeLoadedDistributionConfig(response.config, distributionState.referenceData);
        statsState.isLoaded = false;
        renderDistributionConfigForm();
        setDistributionStatus("Группа распределения сохранена. После перезагрузки конфигурация будет поднята из базы.", "is-success");
      } catch (error) {
        setDistributionStatus(error.message || "Не удалось сохранить конфигурацию группы.", "is-error");
      }
    }

    async function loadStatsData(forceReload) {
      if (statsState.isLoading) {
        return;
      }
      if (statsState.isLoaded && !forceReload) {
        renderStatsData(statsState.data || {});
        return;
      }

      const distributionMemberId = await resolveDistributionMemberId();
      if (!distributionMemberId) {
        setStatsStatus("Не удалось определить member_id портала для загрузки статистики.", "is-error");
        renderStatsEmpty(statsSummaryList, "Нет portal context.");
        renderStatsEmpty(statsMembersList, "Нет portal context.");
        renderStatsEmpty(statsDiagnosticsList, "Нет portal context.");
        renderStatsEmpty(statsJournalList, "Нет portal context.");
        return;
      }

      statsState.isLoading = true;
      setStatsStatus("Загружаем журнал распределения...");
      try {
        await syncPortalContextFromBitrix();
        const payload = await fetchJson(`/api/ui/stats?member_id=${encodeURIComponent(distributionMemberId)}`);
        statsState.data = payload;
        statsState.isLoaded = true;
        renderStatsData(payload);
        setStatsStatus("Журнал и runtime-данные обновлены.", "is-success");
      } catch (error) {
        setStatsStatus(error.message || "Не удалось загрузить статистику.", "is-error");
        renderStatsEmpty(statsSummaryList, "Статистика пока недоступна.");
        renderStatsEmpty(statsMembersList, "Статистика пока недоступна.");
        renderStatsEmpty(statsDiagnosticsList, "Статистика пока недоступна.");
        renderStatsEmpty(statsJournalList, "Статистика пока недоступна.");
      } finally {
        statsState.isLoading = false;
      }
    }

    async function loadDistributionReferenceData() {
      if (distributionState.isLoaded || distributionState.isLoading) {
        if (distributionState.isLoaded) {
          if (distributionState.openFormRequested) {
            renderDistributionConfigForm();
          }
        }
        return;
      }

      const distributionMemberId = await resolveDistributionMemberId();
      if (!distributionMemberId) {
        if (distributionState.openFormRequested) {
          showDistributionLanding();
          setDistributionStatus(
            "Не удалось определить member_id портала. Откройте приложение внутри Bitrix24 или завершите повторную установку, чтобы загрузить реальные справочники.",
            "is-error"
          );
        }
        return;
      }

      distributionState.isLoading = true;
      if (distributionState.openFormRequested) {
        showDistributionForm();
        distributionForm.hidden = true;
        setDistributionStatus("Загружаем данные портала и конфигурацию группы...");
      }

      try {
        await syncPortalContextFromBitrix();
        const [referencePayload, configPayload] = await Promise.all([
          fetchJson(`/api/ui/groups/reference-data?member_id=${encodeURIComponent(distributionMemberId)}`),
          fetchJson(`/api/ui/groups/config?member_id=${encodeURIComponent(distributionMemberId)}`),
        ]);

        distributionState.referenceData = referencePayload;
        distributionState.config = normalizeLoadedDistributionConfig(configPayload.config, referencePayload);
        distributionState.isLoaded = true;
        if (distributionState.openFormRequested) {
          renderDistributionConfigForm();
          if (configPayload.config) {
            setDistributionStatus("Сохраненная конфигурация группы загружена. Изменения применяются кнопкой «Применить».", "is-success");
          } else {
            setDistributionStatus("Справочники Bitrix24 загружены. Заполните форму и нажмите «Применить».", "is-success");
          }
        } else {
          showDistributionLanding();
        }
      } catch (error) {
        if (distributionState.openFormRequested) {
          showDistributionLanding();
          setDistributionStatus(error.message || "Не удалось загрузить форму распределения.", "is-error");
        }
      } finally {
        distributionState.isLoading = false;
      }
    }

    function handleCreateDistributionGroupClick() {
      distributionState.openFormRequested = true;
      loadDistributionReferenceData();
    }

    function setActiveView(view) {
      const content = sectionContent[view] || sectionContent.overview;
      const isDistribution = view === "distribution";
      const isStats = view === "stats";

      defaultPanel.hidden = isDistribution || isStats;
      statsPanel.hidden = !isStats;
      distributionPanel.hidden = !isDistribution;
      mainCard.classList.remove("is-centered");

      if (!isDistribution && !isStats) {
        distributionState.openFormRequested = false;
        sectionBadge.textContent = content.badge;
        sectionTitle.textContent = content.title;
        sectionDescription.textContent = content.description;
      } else if (isDistribution) {
        distributionState.openFormRequested = false;
        showDistributionLanding();
        loadDistributionReferenceData();
      } else {
        loadStatsData(false);
      }

      menuButtons.forEach((button) => {
        button.classList.toggle("is-active", button.dataset.view === view);
      });
    }

    applyBulkLimitButton.addEventListener("click", applyBulkLimitFromForm);
    createDistributionGroupButton.addEventListener("click", handleCreateDistributionGroupClick);
    saveDistributionButton.addEventListener("click", saveDistributionConfig);
    refreshStatsButton.addEventListener("click", () => loadStatsData(true));

    menuButtons.forEach((button) => {
      button.addEventListener("click", () => setActiveView(button.dataset.view));
    });
  </script>
</body>
</html>
"""
    return template.replace("__INITIAL_MEMBER_ID__", initial_member_id_json)


def render_install_page(*, initial_member_id: str | None = None) -> str:
    initial_member_id_json = json.dumps(initial_member_id or "", ensure_ascii=False)
    template = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bitrix App Install</title>
  <script src="//api.bitrix24.tech/api/v1/"></script>
  <style>
    html, body {
      margin: 0;
      min-height: 100%;
      background: #ffffff;
    }
  </style>
</head>
<body>
  <script>
    const initialMemberId = __INITIAL_MEMBER_ID__;
    const canvasUrl = initialMemberId ? `/ui/groups?member_id=${encodeURIComponent(initialMemberId)}` : "/ui/groups";

    if (window.BX24 && typeof window.BX24.installFinish === "function") {
      window.BX24.installFinish();
    }

    window.setTimeout(function () {
      window.location.replace(canvasUrl);
    }, 300);
  </script>
</body>
</html>
"""
    return template.replace("__INITIAL_MEMBER_ID__", initial_member_id_json)
