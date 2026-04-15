from __future__ import annotations

import json


def render_blank_page() -> str:
    return """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>NaimTech | Распределение</title>
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
        <span class="section-badge" id="sectionBadge">Раздел</span>
        <h2 class="canvas-title" id="sectionTitle">Общая информация</h2>
        <p class="canvas-subtitle" id="sectionDescription">
          Стартовый экран приложения. Здесь пока нет бизнес-логики, только базовый каркас интерфейса для дальнейшей разработки.
        </p>
      </section>
    </main>
  </div>

  <script>
    const sectionContent = {
      overview: {
        badge: "Раздел",
        title: "Общая информация",
        description: "Стартовый экран приложения. Здесь пока нет бизнес-логики, только базовый каркас интерфейса для дальнейшей разработки.",
      },
      distribution: {
        badge: "Раздел",
        title: "Распределение сделок",
        description: "Пустой рабочий экран под будущую механику распределения. Сейчас это только навигационный раздел без действий и данных.",
      },
      stats: {
        badge: "Раздел",
        title: "Статистика",
        description: "Заготовка под аналитику и метрики. Раздел уже переключается из меню, но остается чистым холстом.",
      },
      settings: {
        badge: "Раздел",
        title: "Настройки",
        description: "Стартовая точка для будущих конфигураций приложения. Пока здесь нет форм и настроек, только структура интерфейса.",
      },
    };

    const sectionBadge = document.getElementById("sectionBadge");
    const sectionTitle = document.getElementById("sectionTitle");
    const sectionDescription = document.getElementById("sectionDescription");
    const menuButtons = document.querySelectorAll("[data-view]");

    function setActiveView(view) {
      const content = sectionContent[view] || sectionContent.overview;
      sectionBadge.textContent = content.badge;
      sectionTitle.textContent = content.title;
      sectionDescription.textContent = content.description;

      menuButtons.forEach((button) => {
        button.classList.toggle("is-active", button.dataset.view === view);
      });
    }

    menuButtons.forEach((button) => {
      button.addEventListener("click", () => setActiveView(button.dataset.view));
    });
  </script>
</body>
</html>
"""


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
