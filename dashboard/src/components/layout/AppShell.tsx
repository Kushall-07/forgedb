import { useState } from 'react';
import { Outlet } from 'react-router-dom';
import { Sidebar } from './Sidebar';
import { TopBar } from './TopBar';
import './AppShell.css';

export function AppShell() {
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <div className="app-shell">
      <div className="bg-grid" aria-hidden="true" />
      <div className="sc-grain" aria-hidden="true" />

      <a href="#main-content" className="sr-only">
        Skip to main content
      </a>

      <Sidebar mobileOpen={mobileOpen} onNavigate={() => setMobileOpen(false)} />

      {mobileOpen ? (
        <button
          type="button"
          className="app-shell__scrim"
          aria-label="Close navigation"
          onClick={() => setMobileOpen(false)}
        />
      ) : null}

      <div className="app-shell__content">
        <TopBar mobileOpen={mobileOpen} onToggleMobile={() => setMobileOpen((v) => !v)} />
        <main id="main-content" className="app-shell__main">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
