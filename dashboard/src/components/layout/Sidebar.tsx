import { NavLink } from 'react-router-dom';
import { Activity, Database, GitBranch, Share2, Terminal } from 'lucide-react';
import { StatusIndicator } from '../ui/StatusIndicator';
import { TechnicalLabel } from '../ui/TechnicalLabel';
import './Sidebar.css';

interface NavItem {
  to: string;
  label: string;
  icon: typeof Database;
  soon?: boolean;
}

const NAV_ITEMS: NavItem[] = [
  { to: '/', label: 'Overview', icon: Database },
  { to: '/cluster', label: 'Cluster', icon: Share2 },
  { to: '/kv', label: 'KV Console', icon: Terminal },
  { to: '/raft', label: 'Raft', icon: GitBranch },
  { to: '/observability', label: 'Observability', icon: Activity },
];

interface SidebarProps {
  mobileOpen: boolean;
  onNavigate: () => void;
}

export function Sidebar({ mobileOpen, onNavigate }: SidebarProps) {
  return (
    <aside className={`sidebar ${mobileOpen ? 'sidebar--open' : ''}`} aria-label="Primary">
      <div className="sidebar__brand">
        <span className="sidebar__brand-mark" aria-hidden="true">
          <Database size={18} strokeWidth={2.25} />
        </span>
        <span className="sidebar__brand-text">
          <span className="sidebar__brand-name">FORGEDB</span>
          <span className="sidebar__brand-sub mono">Distributed Database</span>
        </span>
      </div>

      <nav className="sidebar__nav">
        <ul>
          {NAV_ITEMS.map((item) => (
            <li key={item.to}>
              <NavLink
                to={item.to}
                end={item.to === '/'}
                onClick={onNavigate}
                className={({ isActive }) => `sidebar__link ${isActive ? 'sidebar__link--active' : ''}`}
              >
                <item.icon size={16} strokeWidth={2} aria-hidden="true" />
                <span className="sidebar__link-label">{item.label}</span>
                {item.soon ? <span className="sidebar__soon mono">SOON</span> : null}
              </NavLink>
            </li>
          ))}
        </ul>
      </nav>

      <div className="sidebar__footer">
        <div className="sidebar__footer-row">
          <TechnicalLabel>System</TechnicalLabel>
          <StatusIndicator tone="sync" label="Healthy" size="sm" pulse />
        </div>
        <div className="sidebar__footer-row">
          <TechnicalLabel>Version</TechnicalLabel>
          <span className="mono sidebar__version">v1.0.0</span>
        </div>
      </div>
    </aside>
  );
}
