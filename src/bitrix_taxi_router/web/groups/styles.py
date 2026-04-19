GROUPS_PAGE_STYLES = """    :root {
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
      flex-wrap: wrap;
      align-items: stretch;
      gap: 24px;
    }

    .distribution-groups-list {
      display: flex;
      flex-wrap: wrap;
      gap: 24px;
      align-items: stretch;
    }

    .distribution-groups-list:empty {
      display: none;
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

    .distribution-group-card {
      width: 340px;
      min-height: 205px;
      border: 1px solid rgba(46, 123, 244, 0.1);
      border-radius: 10px;
      background: #fbfbfe;
      display: flex;
      flex-direction: column;
      gap: 12px;
      padding: 12px;
    }

    .distribution-group-card.is-inactive {
      opacity: 0.84;
    }

    .distribution-group-card-head {
      display: flex;
      gap: 12px;
      align-items: center;
    }

    .distribution-group-title-wrap {
      min-width: 0;
      display: flex;
      flex-wrap: wrap;
      gap: 8px 10px;
      align-items: center;
    }

    .distribution-group-checkbox {
      width: 20px;
      height: 20px;
      flex: 0 0 20px;
      border-radius: 6px;
      border: 1px solid #0070ff;
      background: #0070ff;
      color: #ffffff;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-size: 13px;
      line-height: 1;
      font-weight: 700;
      cursor: pointer;
      padding: 0;
    }

    .distribution-group-card.is-inactive .distribution-group-checkbox {
      background: transparent;
      color: transparent;
      border-color: #cdd6ea;
    }

    .distribution-group-checkbox:disabled {
      cursor: default;
      opacity: 0.7;
    }

    .distribution-group-checkbox:focus-visible {
      outline: 2px solid rgba(46, 123, 244, 0.35);
      outline-offset: 2px;
    }

    .distribution-group-title {
      margin: 0;
      flex: 0 1 auto;
      font-size: 16px;
      line-height: 1.5;
      font-weight: 700;
      color: #333333;
      word-break: break-word;
    }

    .distribution-group-status {
      display: inline-flex;
      flex: 0 0 auto;
      align-items: center;
      min-height: 24px;
      padding: 3px 10px;
      border-radius: 999px;
      font-size: 12px;
      line-height: 1.3;
      font-weight: 600;
    }

    .distribution-group-status.is-active {
      background: rgba(38, 182, 102, 0.12);
      color: #17874a;
    }

    .distribution-group-status.is-inactive {
      background: rgba(235, 72, 77, 0.12);
      color: #c13a3f;
    }

    .distribution-group-subtitle,
    .distribution-group-description {
      margin: 0;
      font-size: 14px;
      line-height: 1.35;
      font-weight: 500;
      color: #5c5c5c;
      word-break: break-word;
    }

    .distribution-group-description strong {
      font-weight: 600;
      color: #333333;
    }

    .distribution-group-actions {
      margin-top: auto;
      display: flex;
      justify-content: flex-start;
      gap: 12px;
      padding-top: 4px;
    }

    .distribution-group-edit {
      min-width: 81px;
      height: 27px;
      border: 0;
      border-radius: 10px;
      background: #2e7bf4;
      color: #ffffff;
      font: inherit;
      font-size: 14px;
      line-height: 1.3;
      font-weight: 500;
      cursor: pointer;
      padding: 0 12px;
    }

    .distribution-group-edit:hover {
      opacity: 0.92;
    }

    .distribution-group-delete {
      min-width: 81px;
      height: 27px;
      border: 0;
      border-radius: 10px;
      background: #eb484d;
      color: #ffffff;
      font: inherit;
      font-size: 14px;
      line-height: 1.3;
      font-weight: 500;
      cursor: pointer;
      padding: 0 12px;
    }

    .distribution-group-delete:hover {
      opacity: 0.92;
    }

    .distribution-group-edit:disabled,
    .distribution-group-delete:disabled {
      cursor: default;
      opacity: 0.7;
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

    .stats-grid--distribution {
      align-items: start;
    }

    .stats-grid--full {
      grid-template-columns: minmax(0, 1fr);
    }

    .stats-summary-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }

    .stats-summary-item {
      min-width: 0;
      padding: 16px 18px;
      border-radius: 16px;
      background: #f7faff;
      border: 1px solid rgba(46, 123, 244, 0.08);
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .stats-summary-value {
      font-size: 28px;
      line-height: 1.1;
      font-weight: 700;
      color: #1f2a44;
    }

    .stats-summary-title {
      font-size: 14px;
      line-height: 1.4;
      font-weight: 600;
      color: #39507c;
    }

    .stats-summary-note {
      font-size: 12px;
      line-height: 1.4;
      color: #7081a8;
    }

    .stats-table-wrap {
      overflow-x: auto;
      border: 1px solid var(--border-soft);
      border-radius: 16px;
      background: #fbfdff;
    }

    .stats-table {
      width: 100%;
      min-width: 720px;
      border-collapse: collapse;
    }

    .stats-table th,
    .stats-table td {
      padding: 14px 16px;
      text-align: left;
      border-bottom: 1px solid rgba(220, 230, 245, 0.9);
      vertical-align: top;
      font-size: 14px;
      line-height: 1.5;
      color: #24324f;
    }

    .stats-table th {
      font-size: 13px;
      font-weight: 700;
      color: #5c6d96;
      background: #f7faff;
      white-space: nowrap;
    }

    .stats-table tbody tr:last-child td {
      border-bottom: 1px solid rgba(220, 230, 245, 0.9);
    }

    .stats-table tfoot td {
      font-weight: 700;
      background: #f7faff;
    }

    .stats-table-primary {
      font-weight: 600;
      color: #1f2a44;
    }

    .stats-table-secondary {
      margin-top: 2px;
      font-size: 12px;
      line-height: 1.4;
      color: #7081a8;
    }

    .stats-table-empty {
      color: #7081a8;
      text-align: center;
    }

    .stats-scroll-area {
      min-height: 0;
      overflow-y: auto;
      padding-right: 4px;
    }

    .stats-scroll-area--journal {
      max-height: 540px;
    }

    .stats-scroll-area--members {
      max-height: 280px;
      margin-top: 12px;
    }

    .stats-scroll-area--diagnostics {
      max-height: 360px;
      margin-top: 12px;
    }

    .stats-technical-stack {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .stats-disclosure {
      border: 1px solid var(--border-soft);
      border-radius: 18px;
      background: #ffffff;
      overflow: hidden;
    }

    .stats-disclosure-summary {
      list-style: none;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 18px 20px;
      cursor: pointer;
      font-size: 16px;
      line-height: 1.4;
      font-weight: 700;
      color: #1f2a44;
    }

    .stats-disclosure-summary::-webkit-details-marker {
      display: none;
    }

    .stats-disclosure-badge {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 3px 10px;
      border-radius: 999px;
      background: rgba(46, 123, 244, 0.08);
      color: var(--brand-blue);
      font-size: 12px;
      line-height: 1.3;
      font-weight: 600;
    }

    .stats-disclosure-content {
      padding: 0 20px 20px;
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

    .reference-pill.is-success {
      background: rgba(38, 182, 102, 0.12);
      color: #17874a;
    }

    .reference-pill.is-warning {
      background: rgba(242, 170, 54, 0.14);
      color: #b87409;
    }

    .reference-pill.is-danger {
      background: rgba(235, 72, 77, 0.12);
      color: #c13a3f;
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

      .distribution-groups-panel,
      .distribution-groups-list {
        flex-direction: column;
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

      .stats-summary-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }"""
